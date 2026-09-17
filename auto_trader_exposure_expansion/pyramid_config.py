"""AutoTrader mixin: SavedTradingConfiguration symbol-list merge/persist
helpers used by the capital pyramid. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
import json
from datetime import datetime, timezone


class PyramidConfigMixin:

    def _persist_saved_configuration_symbols(
        self,
        config_doc: dict,
        symbols_list: list,
        set_path: str,
        updated_count: int,
        configuration_id: str,
    ) -> bool:
        """Legacy single-list persist  prefer _persist_pyramid_merged_updates."""
        return self._persist_pyramid_merged_updates(
            config_doc=config_doc,
            updated_by_symbol={
                self._normalize_config_symbol(entry.get("symbol")): dict(entry)
                for entry in (symbols_list or [])
                if isinstance(entry, dict) and entry.get("symbol")
            },
            removed_active_symbols=set(),
            active_symbols={
                self._normalize_config_symbol(entry.get("symbol"))
                for entry in (symbols_list or [])
                if isinstance(entry, dict) and entry.get("symbol")
            },
            updated_count=updated_count,
            configuration_id=configuration_id,
        )

    def _configuration_symbol_set_paths(self, config_doc: dict) -> list:
        """All Mongo dot-paths for symbol arrays under configuration."""
        paths = []
        root = self._normalize_configuration_root(config_doc)
        if isinstance(root.get("symbols"), list):
            paths.append("configuration.symbols")

        alphas = root.get("alphas")
        if isinstance(alphas, dict) and isinstance(alphas.get("symbols"), list):
            paths.append("configuration.alphas.symbols")
        elif isinstance(alphas, list):
            for idx, alpha in enumerate(alphas):
                if isinstance(alpha, dict) and isinstance(alpha.get("symbols"), list):
                    paths.append(f"configuration.alphas.{idx}.symbols")
        return paths

    def _merge_pyramid_symbol_list(
        self,
        original_list: list,
        updated_by_symbol: dict,
        removed_active_symbols: set,
        active_symbols: set,
    ) -> list:
        """
        Merge pyramid results into a full symbol list.
        - Symbols not in this simulation run are preserved unchanged.
        - Active sim symbols with losses are removed.
        - Active sim winners get updated capital/rank fields.
        """
        active_set = {self._normalize_config_symbol(s) for s in (active_symbols or [])}
        removed_set = {self._normalize_config_symbol(s) for s in (removed_active_symbols or [])}
        result = []
        for entry in original_list or []:
            if not isinstance(entry, dict):
                continue
            sym_key = self._normalize_config_symbol(entry.get("symbol"))
            if not sym_key:
                continue
            if sym_key in removed_set:
                continue
            if sym_key in updated_by_symbol:
                merged = dict(entry)
                merged.update(updated_by_symbol[sym_key])
                result.append(merged)
            else:
                result.append(dict(entry))
        return result

    def _persist_pyramid_merged_updates(
        self,
        config_doc: dict,
        updated_by_symbol: dict,
        removed_active_symbols: set,
        active_symbols: set,
        updated_count: int,
        configuration_id: str,
    ) -> bool:
        config_coll = self._get_saved_trading_configuration_collection(
            getattr(self, "_pyramid_config_collection", None)
        )
        if config_coll is None:
            print("[PYRAMID] Failed to persist  collection unavailable")
            return False

        root = self._normalize_configuration_root(config_doc)
        update_doc = {"pyramid_updated_at": datetime.now(timezone.utc)}
        paths = self._configuration_symbol_set_paths(config_doc)
        if not paths:
            print("[PYRAMID] No symbol list paths found  aborting save")
            return False

        for set_path in paths:
            parts = set_path.split(".")
            node = root
            for part in parts[1:]:
                if not isinstance(node, dict):
                    node = None
                    break
                node = node.get(part)
            if not isinstance(node, list):
                print(f"[PYRAMID] Skip persist path {set_path!r}  list missing")
                continue
            merged_list = self._merge_pyramid_symbol_list(
                node,
                updated_by_symbol,
                removed_active_symbols,
                active_symbols,
            )
            update_doc[set_path] = merged_list
            print(
                f"[PYRAMID] merge {set_path}: "
                f"{len(node)} -> {len(merged_list)} symbol(s)"
            )

        if len(update_doc) <= 1:
            print("[PYRAMID] Nothing to persist after merge")
            return False

        config_db_name = config_coll.database.name
        coll_name = config_coll.name
        query = getattr(self, "_pyramid_config_filter", None) or {"_id": config_doc.get("_id")}
        result = config_coll.update_one(query, {"$set": update_doc})
        print(
            f"[PYRAMID] Mongo save to {config_db_name}.{coll_name} "
            f"matched={result.matched_count} modified={result.modified_count}"
        )

        if result.matched_count == 0:
            print(f"[PYRAMID] Save failed  no document matched filter={query}")
            return False

        if result.modified_count:
            print(
                f"[PYRAMID] SavedTradingConfiguration updated "
                f"({updated_count} profitable symbol(s) merged)"
            )
            if updated_count > 0:
                self.alerts.notify(
                    f"Capital pyramid applied for {updated_count} symbol(s) in configuration {configuration_id}"
                )
            return True

        print("[PYRAMID] Document matched but capital values unchanged (already saved?)")
        return True

    @staticmethod
    def _normalize_config_symbol(symbol: str) -> str:
        return (symbol or "").upper().replace("-EQ", "").strip()

    def _normalize_configuration_root(self, config_doc: dict) -> dict:
        root = config_doc.get("configuration", config_doc)
        if isinstance(root, str):
            try:
                root = json.loads(root)
            except Exception as exc:
                print(f"[PYRAMID] configuration JSON parse failed: {exc}")
                return {}
        if not isinstance(root, dict):
            print(f"[PYRAMID] configuration root is {type(root).__name__}, expected dict")
            return {}
        print(f"[PYRAMID] configuration keys: {list(root.keys())}")
        return root

    def _get_configuration_symbols_ref(self, config_doc: dict):
        """
        Returns (symbols_list, set_path_prefix) where set_path_prefix is used for $set.
        symbols_list is the mutable list inside config_doc.
        """
        root = self._normalize_configuration_root(config_doc)
        alphas = root.get("alphas")

        if isinstance(alphas, dict) and isinstance(alphas.get("symbols"), list):
            return alphas["symbols"], "configuration.alphas.symbols"

        if isinstance(alphas, list):
            for idx, alpha in enumerate(alphas):
                if isinstance(alpha, dict) and isinstance(alpha.get("symbols"), list):
                    return alpha["symbols"], f"configuration.alphas.{idx}.symbols"

        if isinstance(root.get("symbols"), list):
            return root["symbols"], "configuration.symbols"

        return None, None

    def _iter_configuration_symbol_lists(self, config_doc: dict):
        """Yield every symbols array stored under configuration / alphas."""
        root = self._normalize_configuration_root(config_doc)
        symbols = root.get("symbols")
        if isinstance(symbols, list):
            yield symbols

        alphas = root.get("alphas")
        if isinstance(alphas, dict):
            alpha_symbols = alphas.get("symbols")
            if isinstance(alpha_symbols, list):
                yield alpha_symbols
        elif isinstance(alphas, list):
            for alpha in alphas:
                if isinstance(alpha, dict) and isinstance(alpha.get("symbols"), list):
                    yield alpha["symbols"]

    def _build_configuration_rank_map(self, config_doc: dict) -> dict:
        """Map SYMBOL -> rank from configuration.symbols (master) and alphas.symbols."""
        root = self._normalize_configuration_root(config_doc)
        rank_map = {}

        master_symbols = root.get("symbols")
        if isinstance(master_symbols, list):
            print(f"[PYRAMID] configuration.symbols entries={len(master_symbols)}")
            for idx, entry in enumerate(master_symbols):
                if not isinstance(entry, dict):
                    continue
                sym_key = self._normalize_config_symbol(entry.get("symbol"))
                if not sym_key:
                    continue
                rank = entry.get("rank")
                if rank is None:
                    rank = idx + 1
                    print(f"[PYRAMID] {sym_key}: rank missing in configuration.symbols  using order rank={rank}")
                try:
                    rank_map[sym_key] = int(rank)
                except (TypeError, ValueError):
                    print(f"[PYRAMID] {sym_key}: invalid rank={rank!r} in configuration.symbols")

        alphas = root.get("alphas")
        alpha_symbols = alphas.get("symbols") if isinstance(alphas, dict) else None
        if isinstance(alpha_symbols, list):
            print(f"[PYRAMID] configuration.alphas.symbols entries={len(alpha_symbols)}")
            for idx, entry in enumerate(alpha_symbols):
                if not isinstance(entry, dict):
                    continue
                sym_key = self._normalize_config_symbol(entry.get("symbol"))
                if not sym_key or sym_key in rank_map:
                    continue
                rank = entry.get("rank")
                if rank is None:
                    rank = idx + 1
                try:
                    rank_map[sym_key] = int(rank)
                except (TypeError, ValueError):
                    pass

        print(f"[PYRAMID] rank_map built for {len(rank_map)} symbol(s): {rank_map}")
        return rank_map

    def _build_configuration_entry_templates(self, config_doc: dict) -> dict:
        """Best-effort symbol -> config entry template from all config symbol lists."""
        templates = {}
        for entries in self._iter_configuration_symbol_lists(config_doc):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                sym_key = self._normalize_config_symbol(entry.get("symbol"))
                if not sym_key:
                    continue
                merged = dict(templates.get(sym_key) or {})
                merged.update(entry)
                templates[sym_key] = merged
        return templates

    def _get_pyramid_active_symbol_keys(self) -> list:
        """Symbols that were actually part of this simulation run."""
        active = set()

        allocations = getattr(self, "symbol_allocations", None) or {}
        if isinstance(allocations, dict):
            for sym in allocations.keys():
                sym_key = self._normalize_config_symbol(sym)
                if sym_key:
                    active.add(sym_key)

        initial_allocations = getattr(self, "initial_allocations", None) or {}
        if isinstance(initial_allocations, dict):
            for sym in initial_allocations.keys():
                sym_key = self._normalize_config_symbol(sym)
                if sym_key:
                    active.add(sym_key)

        try:
            with self._paper_lock:
                for sym in self._paper_positions.keys():
                    sym_key = self._normalize_config_symbol(sym)
                    if sym_key:
                        active.add(sym_key)
        except Exception:
            pass

        return sorted(active)

    def _resolve_symbol_capital(self, sym_key: str, entry: dict) -> float:
        try:
            cap = float(entry.get("capital") or 0.0)
        except (TypeError, ValueError):
            cap = 0.0
        if cap > 0:
            return cap

        allocations = getattr(self, "symbol_allocations", None) or {}
        raw_alloc = allocations.get(sym_key)
        if isinstance(raw_alloc, dict):
            try:
                return float(raw_alloc.get("capital") or 0.0)
            except (TypeError, ValueError):
                return 0.0
        if raw_alloc is not None:
            try:
                return float(raw_alloc)
            except (TypeError, ValueError):
                return 0.0

        initial_allocations = getattr(self, "initial_allocations", None) or {}
        init_entry = initial_allocations.get(sym_key)
        if isinstance(init_entry, dict):
            try:
                return float(init_entry.get("capital") or 0.0)
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    def _build_pyramid_config_entries(self, config_doc: dict, symbols_list: list, rank_map: dict):
        templates = self._build_configuration_entry_templates(config_doc)
        entry_by_sym = {}
        for entry in symbols_list or []:
            if not isinstance(entry, dict):
                continue
            sym_key = self._normalize_config_symbol(entry.get("symbol"))
            if sym_key:
                entry_by_sym[sym_key] = dict(entry)

        active_symbols = self._get_pyramid_active_symbol_keys()
        if not active_symbols:
            active_symbols = sorted(entry_by_sym.keys())
            print(f"[PYRAMID] no active allocations  falling back to symbols_list ({len(active_symbols)} symbol(s))")
        else:
            print(f"[PYRAMID] active simulation symbols: {active_symbols}")

        config_entries = []
        for sym_key in active_symbols:
            entry = dict(templates.get(sym_key) or entry_by_sym.get(sym_key) or {})
            entry["symbol"] = sym_key

            cap = self._resolve_symbol_capital(sym_key, entry)
            if cap <= 0:
                print(f"[PYRAMID] {sym_key}: no capital  skipped")
                continue
            entry["capital"] = cap

            rank = entry.get("rank")
            if rank is None and sym_key in rank_map:
                entry["rank"] = rank_map[sym_key]
                print(f"[PYRAMID] {sym_key}: rank resolved from config -> {entry['rank']}")

            config_entries.append((sym_key, entry, cap))

        return config_entries

