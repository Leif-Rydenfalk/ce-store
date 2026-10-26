//! ce-store: content-addressed, erasure-coded storage for the ce mesh.
//!
//! Primitives only — no network, no filesystem:
//!   cid    — BLAKE3 content ids, rendered as `b3:<64 hex>`
//!   chunk  — fixed-size chunking into a manifest
//!   ec     — Reed–Solomon k-of-n shards per file
//!   place  — rendezvous placement across cells with failure domains
//!   wire   — the binary frame codec (ce-wire v2, not HTTP)
//!
//! Manifests and shards are immutable, so replication and repair are idempotent: a shard is
//! bytes whose hash is known, and any k of n reconstruct the file.

pub mod cid {
    use std::fmt;

    /// BLAKE3-256 content id. `b3:` is the same prefix ce-net already uses for bundles.
    #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct Cid(pub [u8; 32]);

    impl Cid {
        pub fn of(bytes: &[u8]) -> Cid {
            Cid(*blake3::hash(bytes).as_bytes())
        }

        pub fn from_hex(s: &str) -> Option<Cid> {
            let s = s.strip_prefix("b3:").unwrap_or(s);
            if s.len() != 64 {
                return None;
            }
            let mut out = [0u8; 32];
            for i in 0..32 {
                out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
            }
            Some(Cid(out))
        }
    }

    impl fmt::Display for Cid {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "b3:")?;
            for b in self.0.iter() {
                write!(f, "{:02x}", b)?;
            }
            Ok(())
        }
    }

    impl fmt::Debug for Cid {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self)
        }
    }
}

pub mod chunk {
    use crate::cid::Cid;

    /// A file is split into `k` equal shards, encoded to `k + m`, and named by a manifest.
    /// Equal shards keep the overhead exactly (k+m)/k instead of padding every chunk to the
    /// largest one. Streaming reads use byte ranges inside a shard, not chunk ids.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Manifest {
        pub version: u8,
        pub size: u64,
        pub k: u8,
        pub m: u8,
        pub shard_len: u32,
        pub shards: Vec<Cid>,
    }

    impl Manifest {
        pub fn build(data: &[u8], k: usize, m: usize) -> (Manifest, Vec<Vec<u8>>) {
            let shard_len = (data.len() + k - 1) / k.max(1);
            let mut parts: Vec<Vec<u8>> = (0..k).map(|i| {
                let start = i * shard_len;
                let end = (start + shard_len).min(data.len());
                let mut v = if start < data.len() { data[start..end].to_vec() } else { Vec::new() };
                v.resize(shard_len, 0);
                v
            }).collect();
            let (shards, _len) = crate::ec::encode(&parts, m);
            parts.clear();
            let cids: Vec<Cid> = shards.iter().map(|s| Cid::of(s)).collect();
            (Manifest { version: 2, size: data.len() as u64, k: k as u8, m: m as u8,
                        shard_len: shard_len as u32, shards: cids }, shards)
        }

        pub fn encode(&self) -> Vec<u8> {
            let mut out = Vec::new();
            out.push(self.version);
            out.extend_from_slice(&self.size.to_le_bytes());
            out.push(self.k);
            out.push(self.m);
            out.extend_from_slice(&self.shard_len.to_le_bytes());
            for c in &self.shards {
                out.extend_from_slice(&c.0);
            }
            out
        }

        pub fn cid(&self) -> Cid {
            Cid::of(&self.encode())
        }
    }
}

pub mod ec {
    use reed_solomon_erasure::galois_8::ReedSolomon;

    /// Encode one stripe: k data shards -> k data + m parity, all the same length.
    pub fn encode(data: &[Vec<u8>], m: usize) -> (Vec<Vec<u8>>, usize) {
        let k = data.len();
        let len = data.iter().map(|d| d.len()).max().unwrap_or(0).max(1);
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(k + m);
        for d in data {
            let mut v = d.clone();
            v.resize(len, 0);
            shards.push(v);
        }
        for _ in 0..m {
            shards.push(vec![0u8; len]);
        }
        let rs = ReedSolomon::new(k, m).expect("k,m valid");
        rs.encode(&mut shards).expect("encode");
        (shards, len)
    }

    /// Rebuild the k data shards from any k of the k+m shards (None = missing).
    pub fn decode(mut shards: Vec<Option<Vec<u8>>>, k: usize, m: usize) -> Option<Vec<Vec<u8>>> {
        let rs = ReedSolomon::new(k, m).ok()?;
        if shards.iter().filter(|s| s.is_some()).count() < k {
            return None;
        }
        rs.reconstruct(&mut shards).ok()?;
        Some(shards.into_iter().take(k).map(|s| s.unwrap_or_default()).collect())
    }
}

pub mod place {
    use crate::cid::Cid;

    #[derive(Clone, Debug)]
    pub struct Cell {
        pub id: u64,
        pub region: u8,
        pub rack: u8,
        pub capacity_gb: u32,
        pub online: bool,
    }

    /// Rendezvous (highest-random-weight) score; capacity weights the choice.
    fn score(cell: &Cell, shard: &Cid) -> u128 {
        let mut h = blake3::Hasher::new();
        h.update(&cell.id.to_le_bytes());
        h.update(&shard.0);
        let d = h.finalize();
        let mut w = [0u8; 16];
        w.copy_from_slice(&d.as_bytes()[..16]);
        u128::from_le_bytes(w) * (cell.capacity_gb as u128)
    }

    /// Spread shards over failure domains: at most ceil(n_shards / n_domains) per (region, rack),
    /// preferring empty domains and then the highest rendezvous weight. With 16 shards and 12
    /// domains, four domains hold two shards each — and losing any one domain still leaves k.
    pub fn assign(cells: &[Cell], shards: &[Cid]) -> Vec<Option<usize>> {
        let mut domains: Vec<(u8, u8)> = Vec::new();
        for c in cells {
            if c.online && !domains.contains(&(c.region, c.rack)) {
                domains.push((c.region, c.rack));
            }
        }
        if domains.is_empty() {
            return vec![None; shards.len()];
        }
        let cap = (shards.len() + domains.len() - 1) / domains.len();
        let mut load: Vec<usize> = vec![0; domains.len()];
        let mut used_cells: Vec<usize> = Vec::new();
        let mut out = Vec::with_capacity(shards.len());
        for s in shards {
            let mut best: Option<(usize, std::cmp::Reverse<u128>, usize)> = None;
            for (di, _d) in domains.iter().enumerate() {
                if load[di] >= cap {
                    continue;
                }
                let mut pick: Option<usize> = None;
                for (i, c) in cells.iter().enumerate() {
                    if !c.online || (c.region, c.rack) != domains[di] {
                        continue;
                    }
                    if pick.is_none() || score(c, s) > score(&cells[pick.unwrap()], s) {
                        pick = Some(i);
                    }
                }
                if let Some(ci) = pick {
                    if used_cells.contains(&ci) {
                        continue;
                    }
                    let sc = score(&cells[ci], s);
                    let cand = (load[di], std::cmp::Reverse(sc), ci);
                    if best.map(|b| cand < b).unwrap_or(true) {
                        best = Some(cand);
                    }
                }
            }
            match best {
                Some((di, _, ci)) => {
                    load[di] += 1;
                    used_cells.push(ci);
                    out.push(Some(ci));
                }
                None => out.push(None),
            }
        }
        out
    }
}

pub mod wire {
    //! ce-wire v2: length-prefixed binary frames. No headers, no URLs, no HTTP.
    //!
    //!   frame := varint(body_len) body
    //!   body  := u8 op, u16 flags, varint stream, payload[body_len-4]
    //!
    //! varint is unsigned LEB128. `body_len > MAX_FRAME` is refused before allocation, so a
    //! hostile length cannot exhaust memory.

    pub const MAX_FRAME: usize = 8 * 1024 * 1024;

    #[derive(Clone, Debug, PartialEq)]
    pub struct Frame {
        pub op: u8,
        pub flags: u16,
        pub stream: u64,
        pub payload: Vec<u8>,
    }

    pub mod op {
        pub const HELLO: u8 = 1;
        pub const OPEN: u8 = 2;
        pub const GET: u8 = 3;
        pub const WANT: u8 = 4;
        pub const DATA: u8 = 5;
        pub const ACK: u8 = 6;
        pub const HAVE: u8 = 7;
        pub const SUB: u8 = 8;
        pub const PUB: u8 = 9;
        pub const PING: u8 = 10;
        pub const PONG: u8 = 11;
        pub const REDIRECT: u8 = 12;
        pub const ERROR: u8 = 13;
    }

    fn put_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }

    fn get_varint(buf: &[u8], at: &mut usize) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = *buf.get(*at)?;
            *at += 1;
            if shift > 63 {
                return None;
            }
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
        }
    }

    impl Frame {
        pub fn encode(&self) -> Vec<u8> {
            let mut body = Vec::with_capacity(11 + self.payload.len());
            body.push(self.op);
            body.extend_from_slice(&self.flags.to_le_bytes());
            put_varint(&mut body, self.stream);
            body.extend_from_slice(&self.payload);
            let mut out = Vec::with_capacity(body.len() + 4);
            put_varint(&mut out, body.len() as u64);
            out.extend_from_slice(&body);
            out
        }

        pub fn decode(buf: &[u8]) -> Result<(Frame, usize), &'static str> {
            let mut at = 0;
            let len = get_varint(buf, &mut at).ok_or("truncated length")? as usize;
            if len > MAX_FRAME {
                return Err("frame too large");
            }
            let end = at.checked_add(len).ok_or("length overflow")?;
            if end > buf.len() {
                return Err("truncated frame");
            }
            let body = &buf[at..end];
            if body.len() < 4 {
                return Err("frame shorter than header");
            }
            let op = body[0];
            let flags = u16::from_le_bytes([body[1], body[2]]);
            let mut p = 3;
            let stream = get_varint(body, &mut p).ok_or("bad stream id")?;
            Ok((Frame { op, flags, stream, payload: body[p..].to_vec() }, end))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn roundtrip() {
            let f = Frame { op: op::WANT, flags: 3, stream: 1 << 40, payload: vec![9; 1000] };
            let bytes = f.encode();
            let (g, n) = Frame::decode(&bytes).unwrap();
            assert_eq!(f, g);
            assert_eq!(n, bytes.len());
        }

        #[test]
        fn two_frames_in_one_buffer() {
            let a = Frame { op: op::PING, flags: 0, stream: 1, payload: vec![] }.encode();
            let b = Frame { op: op::PONG, flags: 0, stream: 1, payload: vec![1, 2, 3] }.encode();
            let mut buf = a.clone();
            buf.extend_from_slice(&b);
            let (f1, n1) = Frame::decode(&buf).unwrap();
            let (f2, _) = Frame::decode(&buf[n1..]).unwrap();
            assert_eq!(f1.op, op::PING);
            assert_eq!(f2.op, op::PONG);
            assert_eq!(f2.payload, vec![1, 2, 3]);
        }

        #[test]
        fn hostile_length_is_refused() {
            let mut buf = vec![0xff, 0xff, 0xff, 0xff, 0x7f];
            buf.extend_from_slice(&[1, 0, 0, 0]);
            assert_eq!(Frame::decode(&buf), Err("frame too large"));
        }

        #[test]
        fn truncation_is_refused() {
            let f = Frame { op: op::DATA, flags: 0, stream: 1, payload: vec![0; 32] }.encode();
            for cut in 1..f.len() {
                assert!(Frame::decode(&f[..cut]).is_err());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cid_roundtrip() {
        let c = cid::Cid::of(b"hello");
        assert!(c.to_string().starts_with("b3:"));
        assert_eq!(cid::Cid::from_hex(&c.to_string()), Some(c));
    }

    #[test]
    fn ec_survives_m_losses() {
        let data: Vec<Vec<u8>> = (0..12).map(|i| vec![i as u8; 1000]).collect();
        let (shards, _len) = ec::encode(&data, 4);
        assert_eq!(shards.len(), 16);
        let mut s: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        for i in [0, 5, 9, 15] { s[i] = None; }
        let out = ec::decode(s, 12, 4).expect("decodes with four losses");
        assert_eq!(out[0], data[0]);
        let mut s: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        for i in [0, 5, 9, 15, 3] { s[i] = None; }
        assert!(ec::decode(s, 12, 4).is_none());
    }
}
