from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    schema: str = "fmcg_lakehouse"
    landing_volume: str = "/Volumes/workspace/fmcg_lakehouse/landing"

    @property
    def database(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.database}.{name}"
