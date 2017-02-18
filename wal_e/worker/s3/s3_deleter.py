import csv
import sys

from wal_e import exception
from wal_e import retries
from wal_e.worker.base import _Deleter


class Deleter(_Deleter):

    @retries.retry(retries.critical_stop_exception_processor)
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
        result = bucket.delete_keys([key.name for key in page])

        if result and hasattr(result, 'errors'):
            if len(result.errors) > 0:
                w_csv = csv.writer(sys.stdout, dialect='excel-tab')
                w_csv.writerow(('key', 'error_code', 'error_message'))
                for error in result.errors:
                    w_csv.writerow((error.key, error.code,
                                    error.message.replace('\n', ' ')))
                sys.stdout.flush()
                raise exception.UserCritical(
                    msg='Some keys were not deleted',
                    detail=('Failed keys: first {0}, last {1}, {2} total'
                            .format(result.errors[0], result.errors[-1],
                                    len(result.errors))))
