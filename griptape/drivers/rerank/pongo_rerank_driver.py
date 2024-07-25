from __future__ import annotations

from typing import TYPE_CHECKING

from attrs import Factory, define, field

from griptape.drivers import BaseRerankDriver
from griptape.utils import import_optional_dependency

if TYPE_CHECKING:
    from pongo import PongoClient

    from griptape.artifacts import TextArtifact


@define(kw_only=True)
class PongoRerankDriver(BaseRerankDriver):
    api_key: str = field(metadata={"serializable": True})
    num_results: int = field(default=10)
    vec_sample_size: int = field(default=25)
    plaintext_sample_size: int = field(default=5)
    public_metadata_field: str = field(default="metadata")
    key_field: str = field(default="id")
    text_field: str = field(default="text")
    version: str = field(default="v1")
    region: str = field(default="us-west-2")
    client: PongoClient = field(
        default=Factory(lambda self: import_optional_dependency("pongo").PongoClient(self.api_key), takes_self=True),
    )

    def run(self, query: str, artifacts: list[TextArtifact]) -> list[TextArtifact]:
        docs = [
            {self.key_field: str(hash(a.value)), self.text_field: a.value, self.public_metadata_field: {}}
            for a in artifacts
        ]

        response = self.client.filter(
            query=query,
            docs=docs,
            num_results=self.num_results,
            vec_sample_size=self.vec_sample_size,
            plaintext_sample_size=self.plaintext_sample_size,
            public_metadata_field=self.public_metadata_field,
            key_field=self.key_field,
            text_field=self.text_field,
            version=self.version,
            region=self.region,
        )

        artifacts_dict = {str(hash(a.value)): a for a in artifacts}
        return [artifacts_dict[r[self.key_field]] for r in response.json()]
