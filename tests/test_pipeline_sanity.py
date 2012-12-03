import wal_e.pipeline as pipeline


def create_bogus_payload(dirname):
    payload = 'abcd' * 1048576
    payload_file = dirname.join('payload')
    payload_file.write(payload)
    return payload, payload_file


def test_rate_limit(tmpdir):
    payload, payload_file = create_bogus_payload(tmpdir)

    pl = pipeline.PipeViwerRateLimitFilter(1048576 * 100,
                                           stdin=payload_file.open())
    pl.start()
    round_trip = pl.stdout.read()
    pl.finish()
    assert round_trip == payload


def test_upload_download_pipeline(tmpdir, rate_limit):
    payload, payload_file = create_bogus_payload(tmpdir)

    # Upload section
    test_upload = tmpdir.join('upload')
    with open(unicode(test_upload), 'w') as upload:
        with open(unicode(payload_file)) as inp:
            pl = pipeline.get_upload_pipeline(
                inp, upload, rate_limit=rate_limit)
            pl.finish()

    with open(unicode(test_upload)) as completed:
        round_trip = completed.read()

    # Download section
    test_download = tmpdir.join('download')
    with open(unicode(test_upload)) as upload:
        with open(unicode(test_download), 'w') as download:
            pl = pipeline.get_download_pipeline(upload, download)
            pl.finish()

    with open(unicode(test_download)) as completed:
        round_trip = completed.read()

    assert round_trip == payload


def pytest_generate_tests(metafunc):
    # Test both with and without rate limiting if there is rate_limit
    # parameter.
    if "rate_limit" in metafunc.funcargnames:
        metafunc.parametrize("rate_limit", [None, int(2 ** 25)])
