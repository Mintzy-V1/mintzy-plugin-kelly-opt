"""AutoTrader mixin: SavedTradingConfiguration lookups and leverage-multiplier
resolution. Moved verbatim from auto_trader_exposure_expansion.py during the
package split; no logic changes.
"""
import os
from typing import Optional

from .constants import (
    DEFAULT_MONGO_CONFIG_DB_NAME,
    SAVED_TRADING_CONFIGURATION_COLLECTION,
    SAVED_TRADING_CONFIGURATION_COLLECTION_CANDIDATES,
)


class LeverageMixin:

    def _resolve_config_db_name(self) -> str:
        return (
            getattr(self, "config_db_name", None)
            or os.environ.get("MONGO_CONFIG_DB_NAME")
            or DEFAULT_MONGO_CONFIG_DB_NAME
        )

    def _get_saved_trading_configuration_collection(self, collection_name=None):
        coll = self.trading_logs_collection

        if coll is None:
            return None
        config_db_name = self._resolve_config_db_name()
        name = collection_name or SAVED_TRADING_CONFIGURATION_COLLECTION
        if config_db_name != coll.database.name:
            return coll.database.client[config_db_name][name]
        return coll.database[name]

    @staticmethod
    def _coerce_leverage_multiplier(value, source: str = "") -> Optional[float]:
        if value is None:
            return None
        try:
            mult = float(value)
            if mult > 0:
                return mult
            print(f"[LEVERAGE] Ignoring non-positive {source}: {value!r}")
        except (TypeError, ValueError):
            print(f"[LEVERAGE] Invalid {source}: {value!r}")
        return None

    def _leverage_from_config_doc(self, config_doc: dict) -> Optional[float]:
        if not isinstance(config_doc, dict):
            return None
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(
                config_doc.get(key), f"SavedTradingConfiguration.{key}"
            )
            if val is not None:
                return val
        root = self._normalize_configuration_root(config_doc)
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(root.get(key), f"configuration.{key}")
            if val is not None:
                return val
        return None

    def _resolve_leverage_multiplier(self, config_doc=None, default: float = 1.0) -> float:
        """
        Resolve intraday leverage: session/request -> SavedTradingConfiguration -> default.
        Pyramid shutdown uses default=1.0 when nothing else is configured.
        """
        val = self._coerce_leverage_multiplier(
            getattr(self, "leverage_multiplier", None), "session"
        )
        if val is not None:
            self.leverage_multiplier = val
            return val

        doc = config_doc
        if doc is None:
            configuration_id = getattr(self, "configuration_id", None)
            if configuration_id:
                doc = self._fetch_saved_trading_configuration(configuration_id)

        if doc:
            val = self._leverage_from_config_doc(doc)
            if val is not None:
                self.leverage_multiplier = val
                print(f"[LEVERAGE] Using leverage_multiplier={val} from SavedTradingConfiguration")
                return val

        self.leverage_multiplier = float(default)
        print(f"[LEVERAGE] Falling back to leverage_multiplier={default}")
        return float(default)

    def _pyramid_lookup_filters(self, configuration_id: str):
        """Build Mongo queries for SavedTradingConfiguration lookup."""
        filters = []
        object_id = None
        try:
            from bson import ObjectId

            if ObjectId.is_valid(configuration_id):
                object_id = ObjectId(configuration_id)
                filters.append({"_id": object_id})
                print(f"[PYRAMID] ObjectId parsed OK: {object_id}")
            else:
                print(
                    f"[PYRAMID] configuration_id is not valid ObjectId hex: "
                    f"{configuration_id!r} (len={len(configuration_id or '')})"
                )
        except Exception as exc:
            print(f"[PYRAMID] ObjectId parse failed: {type(exc).__name__}: {exc}")

        filters.append({"configuration_id": configuration_id})
        if object_id is not None:
            filters.append({"configuration_id": str(object_id)})

        # De-dupe while preserving order
        seen = set()
        unique = []
        for query in filters:
            key = tuple(sorted((k, str(v)) for k, v in query.items()))
            if key not in seen:
                seen.add(key)
                unique.append(query)
        return unique

    def _pyramid_log_collection_diagnostics(self, mongo_client, config_db_name: str) -> None:
        """Log DB/collection hints when lookup fails."""
        try:
            db = mongo_client[config_db_name]
            all_names = db.list_collection_names()
            related = [
                n for n in all_names
                if "saved" in n.lower() or "trading" in n.lower() or "config" in n.lower()
            ]
            print(
                f"[PYRAMID] DB={config_db_name!r} collections (saved/trading/config related): "
                f"{related or '(none)'}"
            )
            for coll_name in SAVED_TRADING_CONFIGURATION_COLLECTION_CANDIDATES:
                if coll_name not in all_names:
                    print(f"[PYRAMID]   collection {coll_name!r}  NOT PRESENT in this DB")
                    continue
                coll = db[coll_name]
                try:
                    count = coll.estimated_document_count()
                except Exception as count_err:
                    count = f"error:{count_err}"
                sample_ids = []
                try:
                    for doc in coll.find({}, {"_id": 1}).limit(5):
                        sample_ids.append(str(doc.get("_id")))
                except Exception as sample_err:
                    sample_ids = [f"error:{sample_err}"]
                print(
                    f"[PYRAMID]   collection {coll_name!r}  est_docs={count} "
                    f"sample_ids={sample_ids}"
                )
        except Exception as diag_err:
            print(f"[PYRAMID] Collection diagnostics failed: {type(diag_err).__name__}: {diag_err}")

    def _fetch_saved_trading_configuration(self, configuration_id: str) -> Optional[dict]:
        if not configuration_id:
            print("[PYRAMID] configuration_id empty  skip lookup")
            return None

        base_coll = self.trading_logs_collection
        if base_coll is None:
            print("[PYRAMID] trading_logs_collection unavailable  cannot resolve Mongo client")
            return None

        config_db_name = self._resolve_config_db_name()
        logs_db_name = base_coll.database.name
        mongo_client = base_coll.database.client
        env_config_db = os.environ.get("MONGO_CONFIG_DB_NAME")
        trader_config_db = getattr(self, "config_db_name", None)

        print(
            f"[PYRAMID] lookup start configuration_id={configuration_id} "
            f"target_db={config_db_name!r} logs_db={logs_db_name!r} "
            f"trader.config_db_name={trader_config_db!r} env.MONGO_CONFIG_DB_NAME={env_config_db!r}"
        )

        lookup_filters = self._pyramid_lookup_filters(configuration_id)

        for coll_name in SAVED_TRADING_CONFIGURATION_COLLECTION_CANDIDATES:
            config_coll = self._get_saved_trading_configuration_collection(coll_name)
            if config_coll is None:
                print(f"[PYRAMID] collection handle unavailable for {coll_name!r}")
                continue

            try:
                est_count = config_coll.estimated_document_count()
            except Exception as count_err:
                est_count = f"error:{count_err}"

            print(
                f"[PYRAMID] querying {config_db_name}.{coll_name} "
                f"(est_docs={est_count}) with {len(lookup_filters)} filter(s)"
            )

            for idx, query in enumerate(lookup_filters):
                try:
                    doc = config_coll.find_one(query)
                except Exception as find_err:
                    print(
                        f"[PYRAMID]   filter[{idx}] {query} -> ERROR "
                        f"{type(find_err).__name__}: {find_err}"
                    )
                    continue

                status = "FOUND" if doc else "miss"
                print(f"[PYRAMID]   filter[{idx}] {query} -> {status}")
                if doc:
                    self._pyramid_config_filter = query
                    self._pyramid_config_collection = coll_name
                    print(
                        f"[PYRAMID] Found SavedTradingConfiguration "
                        f"db={config_db_name} collection={coll_name} _id={doc.get('_id')}"
                    )
                    return doc

        print(
            f"[PYRAMID] configuration_id not found in {config_db_name}: {configuration_id} "
            f"(tried collections={list(SAVED_TRADING_CONFIGURATION_COLLECTION_CANDIDATES)})"
        )
        self._pyramid_log_collection_diagnostics(mongo_client, config_db_name)
        return None

