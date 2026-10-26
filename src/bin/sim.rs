//! End-to-end simulation: split -> erasure code -> place -> lose cells -> rebuild -> repair.
//!
//! No network: the point is to prove the properties and measure the overhead. Any k of n shards
//! rebuild the file, no two shards share a cell, and repair restores the redundancy budget after
//! cells are lost.

use ce_store::{chunk, cid::Cid, ec, place, wire};
use std::collections::HashMap;
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
}

fn main() {
    let k = 12usize;
    let m = 4usize;
    let n_files = 300usize;
    let mut rng = Rng(0x5EED_1234_ABCD_0001);

    // 3 regions x 4 racks, 4 cells each = 48 cells, 12 failure domains
    let mut cells: Vec<place::Cell> = Vec::new();
    let mut id = 1u64;
    for region in 0..3u8 {
        for rack in 0..4u8 {
            for _ in 0..4 {
                cells.push(place::Cell { id, region, rack, capacity_gb: 1_000, online: true });
                id += 1;
            }
        }
    }

    let mut store: HashMap<(u64, Cid), Vec<u8>> = HashMap::new();
    let mut files: Vec<(Cid, chunk::Manifest, Vec<Vec<u8>>, Cid)> = Vec::new(); // data cid for the check
    let mut logical = 0u64;

    let t0 = Instant::now();
    for _ in 0..n_files {
        let size = 8 * 1024 + rng.below(600 * 1024);
        let mut data = vec![0u8; size];
        for b in data.iter_mut() { *b = rng.next() as u8; }
        logical += size as u64;
        let data_cid = Cid::of(&data);
        let (mf, shards) = chunk::Manifest::build(&data, k, m);
        let assigned = place::assign(&cells, &mf.shards);
        for (i, slot) in assigned.iter().enumerate() {
            let ci = slot.expect("a cell for every shard");
            store.insert((cells[ci].id, mf.shards[i]), shards[i].clone());
        }
        files.push((mf.cid(), mf, shards, data_cid));
    }
    let ingest = t0.elapsed();
    let physical: u64 = store.values().map(|v| v.len() as u64).sum();
    println!("files {}  logical {} KiB  physical {} KiB  overhead {:.2}x  ingest {:?}",
             n_files, logical / 1024, physical / 1024, physical as f64 / logical as f64, ingest);

    // lose 4 cells (any four cells may hold at most m shards of any one file, because no two
    // shards of a file share a cell)
    let mut killed: Vec<u64> = Vec::new();
    let mut lost = 0usize;
    while killed.len() < m {
        let i = rng.below(cells.len());
        if !cells[i].online { continue; }
        cells[i].online = false;
        killed.push(cells[i].id);
        lost += store.keys().filter(|(c, _)| *c == cells[i].id).count();
    }

    // rebuild every file from what survived
    let t1 = Instant::now();
    let mut rebuilt = 0usize;
    for (_cid, mf, _shards, data_cid) in &files {
        let opts: Vec<Option<Vec<u8>>> = mf.shards.iter().map(|c| {
            cells.iter().find_map(|cell| if cell.online { store.get(&(cell.id, *c)).cloned() } else { None })
        }).collect();
        let recovered = ec::decode(opts, k, m).expect("survives four cell losses");
        let mut data = Vec::new();
        for part in recovered {
            data.extend_from_slice(&part);
        }
        data.truncate(mf.size as usize);
        assert_eq!(Cid::of(&data), *data_cid, "content hash after rebuild");
        rebuilt += 1;
    }
    let rebuild = t1.elapsed();

    // repair: put back every shard that no live cell holds
    let t2 = Instant::now();
    let mut repaired = 0usize;
    for (_cid, mf, shards, _d) in &files {
        for (i, c) in mf.shards.iter().enumerate() {
            let present = cells.iter().any(|cell| cell.online && store.contains_key(&(cell.id, *c)));
            if !present {
                let slot = place::assign(&cells, &[*c])[0].expect("a live cell");
                store.insert((cells[slot].id, *c), shards[i].clone());
                repaired += 1;
            }
        }
    }
    let repair = t2.elapsed();

    let physical2: u64 = store.iter().filter(|((cid, _), _)| cells.iter().any(|c| c.id == *cid && c.online))
        .map(|(_, v)| v.len() as u64).sum();
    println!("killed {} cells, lost {} shards, rebuilt {} files in {:?}, repaired {} shards in {:?}",
             killed.len(), lost, rebuilt, rebuild, repaired, repair);
    println!("physical after repair {} KiB ({:.2}x logical)", physical2 / 1024, physical2 as f64 / logical as f64);

    // wire codec throughput
    let t3 = Instant::now();
    let frame = wire::Frame { op: wire::op::DATA, flags: 0, stream: 7, payload: vec![0xAB; 1400] };
    let mut n = 0usize;
    for _ in 0..1_000_000 {
        let bytes = frame.encode();
        let (_f, used) = wire::Frame::decode(&bytes).unwrap();
        n += used;
    }
    let dt = t3.elapsed();
    println!("wire: 1M frames of 1400 B in {:?} = {:.0} frames/s, {:.1} MiB/s",
             dt, 1_000_000.0 / dt.as_secs_f64(), n as f64 / dt.as_secs_f64() / (1024.0 * 1024.0));
}
