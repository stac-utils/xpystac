import pytest

from tests.utils import requires_icechunk


@requires_icechunk
def test_aws_s3_scheme_is_unchanged():
    from xpystac._icechunk import _s3_scheme_kwargs

    scheme = {
        "type": "aws-s3",
        "platform": "https://{bucket}.s3.{region}.amazonaws.com",
        "bucket": "nasa-waterinsight",
        "region": "us-west-2",
        "anonymous": True,
    }
    assert _s3_scheme_kwargs(scheme) == {"region": "us-west-2"}


@requires_icechunk
def test_custom_s3_scheme_path_style():
    from xpystac._icechunk import _s3_scheme_kwargs

    scheme = {
        "type": "custom-s3",
        "platform": "https://rustfs.vm.fedcloud.eu",
        "bucket": "ps-027-10",
    }
    assert _s3_scheme_kwargs(scheme) == {
        "region": None,
        "endpoint_url": "https://rustfs.vm.fedcloud.eu",
        "force_path_style": True,
        "allow_http": False,
    }


@requires_icechunk
def test_custom_s3_scheme_with_region():
    from xpystac._icechunk import _s3_scheme_kwargs

    scheme = {
        "type": "custom-s3",
        "platform": "https://rustfs.vm.fedcloud.eu",
        "bucket": "ps-027-10",
        "region": "us-east-1",
    }
    assert _s3_scheme_kwargs(scheme)["region"] == "us-east-1"


@requires_icechunk
def test_custom_s3_scheme_http_sets_allow_http():
    from xpystac._icechunk import _s3_scheme_kwargs

    scheme = {
        "type": "custom-s3",
        "platform": "http://localhost:9000",
        "bucket": "test",
    }
    assert _s3_scheme_kwargs(scheme)["allow_http"] is True


@requires_icechunk
def test_custom_s3_scheme_virtual_hosted_not_implemented():
    from xpystac._icechunk import _s3_scheme_kwargs

    scheme = {
        "type": "custom-s3",
        "platform": "https://{bucket}.example.com",
        "bucket": "test",
    }
    with pytest.raises(NotImplementedError, match="virtual-hosted-style"):
        _s3_scheme_kwargs(scheme)


@requires_icechunk
def test_unsupported_scheme_type_raises():
    from xpystac._icechunk import _s3_scheme_kwargs

    with pytest.raises(ValueError, match="Unsupported storage scheme type"):
        _s3_scheme_kwargs({"type": "ms-azure"})
