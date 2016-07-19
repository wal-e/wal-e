from wal_e import exception
from wal_e import retries
from wal_e.worker.base import _Deleter


def _on_error(blob):
    # This function is called whenever NotFound is returned from GCS.
    pass


class Deleter(_Deleter):

    @retries.retry()
    def _delete_batch(self, page):
        # Check that all blobs are in the same bucket; this code is not
        # designed to deal with fast deletion of blobs from multiple
        # buckets at the same time, and not checking this could result
        # in deleting similarly named blobs from the wrong bucket.
        #
        # In wal-e's use, homogeneity of the bucket retaining the blobs
        # is presumed to be always the case.
        bucket_name = page[0].bucket.name
        for blob in page:
            if blob.bucket.name != bucket_name:
                raise exception.UserCritical(
                    msg='submitted blobs are not part of the same bucket',
                    detail=('The clashing bucket names are {0} and {1}.'
                            .format(blob.bucket.name, bucket_name)),
                    hint='This should be reported as a bug.')

        bucket = page[0].bucket
        bucket.delete_blobs(page, on_error=_on_error)
