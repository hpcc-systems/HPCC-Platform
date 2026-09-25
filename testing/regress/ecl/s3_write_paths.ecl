// s3_write_paths.ecl — Integration test for the S3 storage-plane write path
//
// Exercises the S3 hook's multipart write coalescing. The hook always uses a
// multipart upload (CreateMultipartUpload + UploadPart(s) + CompleteMultipartUpload):
//  - A tiny file is written as a single-part multipart upload. The sole part is
//    also the final part, so it may be <5MB (which S3 permits).
//  - A large file (more than the ~8MB part threshold) is written as one or more
//    >=8MB parts followed by a smaller final part, so no non-final part falls
//    below S3's 5MB minimum (which would otherwise fail CompleteMultipartUpload
//    with EntityTooSmall).
// Both files are read back to verify data integrity.
//
// Prerequisites: HPCC cluster with an 's3data' storage plane configured.
// Run: ecl run thor s3_write_paths.ecl --server=eclwatch:8010

smallRec := RECORD
    STRING10 val;
END;

// Tiny file — written as a single-part multipart upload
smallDS := DATASET([{'small'}], smallRec);
OUTPUT(smallDS,, '~s3::write_path_small', OVERWRITE, PLANE('s3data'));

bigRec := RECORD
    UNSIGNED8 id;
    STRING100 payload;
END;

// ~108MB raw — spans multiple >=8MB multipart parts plus a smaller final part
bigDS := DATASET(1000000, TRANSFORM(bigRec, SELF.id := COUNTER, SELF.payload := (STRING100)HASH64(COUNTER)));
OUTPUT(bigDS,, '~s3::write_path_large', OVERWRITE, PLANE('s3data'));

// Read back to verify data integrity
OUTPUT(COUNT(DATASET('~s3::write_path_small', smallRec, FLAT)), NAMED('small_count'));
OUTPUT(COUNT(DATASET('~s3::write_path_large', bigRec, FLAT)), NAMED('large_count'));
