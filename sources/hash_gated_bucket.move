module dos_hash_gated_bucket::hash_gated_bucket;

use sui::balance::{Self, Balance};
use sui::coin::Coin;
use sui::table::{Self, Table};
use wal::wal::WAL;
use walrus::blob::Blob;
use walrus::system::System;

public struct HashGatedBucket has key, store {
    id: UID,
    // WAL balance for renewals.
    balance: Balance<WAL>,
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

const ENotInExtensionUnlockWindow: u64 = 1;
const EInvalidAdminCap: u64 = 2;

public fun new(
    extension_epochs: u32,
    extension_unlock_window: u32,
    ctx: &mut TxContext,
): (HashGatedBucket, HashGatedBucketAdminCap) {
    let bucket = HashGatedBucket {
        id: object::new(ctx),
        balance: balance::zero(),
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

public fun deposit_wal(self: &mut HashGatedBucket, coin: Coin<WAL>) {
    self.balance.join(coin.into_balance());
}

public fun withdraw_wal(
    self: &mut HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    value: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.balance.split(value).into_coin(ctx)
}

public fun add_blob(self: &mut HashGatedBucket, cap: &HashGatedBucketAdminCap, blob: Blob) {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    let blob_opt_mut = self.blobs.borrow_mut(blob.blob_id());
    blob_opt_mut.fill(blob);
}

public fun borrow_blob(
    self: &HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_id: u256,
): &Blob {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    let blob_opt = self.blobs.borrow(blob_id);
    blob_opt.borrow()
}

public fun remove_blob(
    self: &mut HashGatedBucket,
    cap: &HashGatedBucketAdminCap,
    blob_id: u256,
): Blob {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.blobs.remove(blob_id).destroy_some()
}

public fun renew_blob(
    self: &mut HashGatedBucket,
    blob_id: u256,
    system: &mut System,
    ctx: &mut TxContext,
) {
    let blob_opt_mut = self.blobs.borrow_mut(blob_id);
    let blob_mut = blob_opt_mut.borrow_mut();

    assert!(
        system.epoch() >= blob_mut.end_epoch() - self.extension_unlock_window,
        ENotInExtensionUnlockWindow,
    );

    let mut payment_coin = self.balance.withdraw_all().into_coin(ctx);
    system.extend_blob(blob_mut, self.extension_epochs, &mut payment_coin);
    self.balance.join(payment_coin.into_balance());
}

public fun renew_blob_with_wal(
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

public fun reserve_blob(self: &mut HashGatedBucket, cap: &HashGatedBucketAdminCap, blob_id: u256) {
    assert!(cap.bucket_id == self.id.to_inner(), EInvalidAdminCap);
    self.blobs.add(blob_id, option::none());
}
