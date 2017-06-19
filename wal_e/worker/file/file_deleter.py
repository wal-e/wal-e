from wal_e import exception
from wal_e import retries
from wal_e.worker.base import _Deleter


class Deleter(_Deleter):

    @retries.retry()
    def _delete_batch(self, page):
        # Check that all keys are in the same bucket; this code is not
        # designed to deal with fast deletion of keys from multiple
        # buckets at the same time, and not checking this could result
        # in deleting similarly named keys from the wrong bucket.
        #
        # In wal-e's use, homogeneity of the bucket retaining the keys
        # is presumed to be always the case.
        bucket_name = page[0].bucket.name
        for key in page:
            if key.bucket.name != bucket_name:
                raise exception.UserCritical(
                    msg='submitted keys are not part of the same bucket',
                    detail=('The clashing bucket names are {0} and {1}.'
                            .format(key.bucket.name, bucket_name)),
                    hint='This should be reported as a bug.')

        bucket = page[0].bucket
        bucket.delete_keys([key.name for key in page])
