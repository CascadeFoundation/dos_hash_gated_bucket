module dos_hash_gated_bucket::hash_gated_bucket;

use sui::coin::Coin;
use sui::table::{Self, Table};
use sui::transfer::Receiving;
use wal::wal::WAL;
use walrus::blob::Blob;
use walrus::system::System;

public struct HashGatedBucket has key, store {
    id: UID,
    // The number of epochs to extend a blob by.
    extension_epochs: u32,
    // The number of epochs before expiration when an extension is allowed.
    extension_unlock_window: u32,
    // The blobs in the bucket.
    blobs: Table<u256, Option<Blob>>,
}

public struct HashGatedBucketAdminCap has key, store {
    id: UID,
    bucket_id: ID,
}

const EBlobKeyNotExists: u64 = 4;
const EBlobNotInBucket: u64 = 3;
const EInvalidAdminCap: u64 = 2;

public fun new(
    extension_epochs: u32,
    extension_unlock_window: u32,
    ctx: &mut TxContext,
): (HashGatedBucket, HashGatedBucketAdminCap) {
    let bucket = HashGatedBucket {
        id: object::new(ctx),
        extension_epochs: extension_epochs,
        extension_unlock_window: extension_unlock_window,
        blobs: table::new(ctx),
    };

    let bucket_admin_cap = HashGatedBucketAdminCap {
        id: object::new(ctx),
        bucket_id: bucket.id.to_inner(),
    };

    (bucket, bucket_admin_cap)
}

// Add a blob to the bucket.
public fun add_blob(self: &mut HashGatedBucket, cap: &HashGatedBucketAdminCap, blob: Blob) {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.blobs.borrow_mut(blob.blob_id()).fill(blob);
}

// Borrow a reference to a stored blob.
public fun borrow_blob(
    self: &HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_id: u256,
): &Blob {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    let blob_opt = self.blobs.borrow(blob_id);
    blob_opt.borrow()
}

// Receive a blob that's been sent directly to the bucket, and add it to the bucket.
public fun receive_and_add_blob(
    self: &mut HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_to_receive: Receiving<Blob>,
) {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    let blob = transfer::public_receive(&mut self.id, blob_to_receive);
    self.blobs.borrow_mut(blob.blob_id()).fill(blob);
}

// Remove a blob from the bucket.
public fun remove_blob(
    self: &mut HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_id: u256,
): Blob {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.blobs.remove(blob_id).destroy_some()
}

// Renew a stored Blob with the provided WAL coin.
public fun renew_blob(
    self: &mut HashGatedBucket,
    blob_id: u256,
    extension_epochs: u32,
    payment_coin: &mut Coin<WAL>,
    system: &mut System,
) {
    let blob_opt_mut = self.blobs.borrow_mut(blob_id);
    let blob_mut = blob_opt_mut.borrow_mut();
    system.extend_blob(blob_mut, extension_epochs, payment_coin);
}

public fun reserve_blob_key(
    self: &mut HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_id: u256,
) {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.blobs.add(blob_id, option::none());
}

public fun assert_blob_key_exists(self: &HashGatedBucket, blob_id: u256) {
    assert!(self.blobs.contains(blob_id), EBlobKeyNotExists);
}

public fun assert_contains_blob(self: &HashGatedBucket, blob_id: u256) {
    assert!(self.blobs.borrow(blob_id).is_some(), EBlobNotInBucket);
}
