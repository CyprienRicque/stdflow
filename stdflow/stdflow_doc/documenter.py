from __future__ import annotations

import copy
import uuid

import pandas as pd
import json

from typing import List, Tuple, Optional, Dict
import logging

from stdflow.stdflow_utils import string_to_uuid

from stdflow_utils.uuid import is_valid_uuid

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

DROPPED = "Dropped"
IMPORTED = "Imported"
CREATED = "Created"

splitter = "::"


class ColStep:
    def __init__(self, alias, col, col_step, input_cols, current_step, id_=None):
        self.alias: str = alias
        self.col: str = col
        self.name: str = col_step
        self.input_cols: List[str] = input_cols
        self.id: str = id_ or str(uuid.uuid4())
        self.current_step: bool = current_step

    def __eq__(self, other: "ColStep"):
        if other is None:
            return False
        return self.id == other.id

    @classmethod
    def from_dict(cls, metadata):
        return cls(
            col=metadata["col"],
            col_step=metadata["col_step"],
            input_cols=metadata["input_cols"],
            id_=metadata["uuid"],
            alias=metadata["alias"],
            current_step=metadata["current_step"],
        )

    def __dict__(self):
        return dict(
            col=self.col,
            col_step=self.name,
            input_cols=self.input_cols,
            uuid=self.id,
            alias=self.alias,
        )

    def __repr__(self):
        return f"({', '.join([a[:4] for a in self.input_cols])}) -> [{self.id[:4]}]{self.alias}{splitter}{self.col} : {self.name}"


class Documenter:
    def __init__(self):
        self.input_df_alias_to_cols = {}  # Used for automatic columns inference
        self.dict_step: Dict[str, ColStep] = {}
        self._step_list: List[ColStep] = []

    def __call__(self, col, col_step, input_cols: list, current_step=True):
        input_cols_with_ids = self._get_input_cols_details(input_cols)
        alias, col = self._get_output_col_details(col, input_cols_with_ids)

        input_steps = [
            self.find_advanced_col_step(col_name, df_alias)
            for df_alias, col_name in input_cols_with_ids
        ]
        # if one of the step is None, raise an error
        if None in input_steps:
            raise ValueError(f"Column {col} has no documented origin.")
        input_uuids = [step.id for step in input_steps if step is not None]

        step = ColStep(
            alias=alias,
            col=col,
            col_step=col_step,
            input_cols=input_uuids,
            current_step=current_step,
        )
        self._add_step(step)

    def _add_step(self, step: ColStep):
        self.dict_step[step.id] = step
        self._step_list.append(step)

    def dict_for_alias(self, alias):
        """
        return all steps used to create columns for a given alias. recursively.
        :return:
        """
        steps = []
        for step in self.step_list:
            if step.alias == alias:
                steps.append(step)

        all_steps_used = []
        for step in steps:
            all_steps_used += self.trace_col_origin(step.id)
            step.id = str(step.id)

        for step in steps:
            step.alias = None

        return steps

    def add_step_simplified(
        self, alias, col, col_step, input_steps: List[ColStep], current_step=True, return_=False
    ):
        step = ColStep(
            alias=alias,
            col=col,
            col_step=col_step,
            input_cols=[step.id for step in input_steps],
            current_step=current_step,
        )
        if not return_:
            self._add_step(step)
        else:
            return step

    def infer_alias_from_col_name(self, col) -> str:
        """
        Infer the alias from the col name.
        :param col:
        :return:
        """
        for alias, cols in self.input_df_alias_to_cols.items():
            if col in cols:
                return alias
        raise ValueError(f"Could not infer alias from column '{col}'.")

    def _get_output_col_details(self, col: str, input_cols: list[tuple[str, str]]) -> Tuple[str, str]:
        # if not alias in out col, infer it from input cols
        if splitter not in col:
            alias: str = input_cols[0][0]
            return alias, col
        # pydantic assert if tuple(col.split(splitter)) is not type Tuple[str, str] TODO

        return tuple(col.split(splitter))

    def _get_input_cols_details(self, input_cols) -> List[Tuple[str, str]]:
        input_cols_with_ids = []
        for input_col in input_cols:
            if splitter in input_col:
                df_alias, col_name = input_col.split(splitter)
            else:
                col_name = input_col
                df_alias = self._infer_dataframe_alias(col_name)

            input_cols_with_ids.append((df_alias, col_name))
        return input_cols_with_ids

    def set_dataframe(self, columns, col_steps: List[dict], alias: str):
        # if alias already exists, raise an error
        # if alias is not None and alias in [step.alias for step in self.step_list]:
        #     raise ValueError(f"Alias {alias} already exists.")
        if col_steps:
            self._add_steps_from_metadata(col_steps, alias)
        steps = self.last_steps_for_each_col(columns, alias)

        # Add missing cols
        missing_cols = [col for col in columns if col not in [step.col for step in steps]]
        if alias == "tmp" and missing_cols:
            logger.error(f"Columns {missing_cols} do not have origin documentation.")
        for col in missing_cols:
            self(f"{alias}{splitter}{col}", IMPORTED, [])
        # End add missing cols
        if alias not in self.input_df_alias_to_cols:
            self.input_df_alias_to_cols[alias] = columns
        else:
            if len(set(self.input_df_alias_to_cols[alias] + columns)) != len(self.input_df_alias_to_cols[alias] + columns):
                duplicated_cols = [col for col in columns if col in self.input_df_alias_to_cols[alias]]
                logger.warning(f"Columns {duplicated_cols} are duplicated in dataframe {alias}.")
            self.input_df_alias_to_cols[alias] += columns

    def _add_steps_from_metadata(self, col_steps: List[dict], alias):
        for step in col_steps:
            step["current_step"] = False
            step["alias"] = alias
            self._add_step(ColStep.from_dict(step))

    def _infer_dataframe_alias(self, col_name):
        matching_df_aliases = [
            df_alias
            for df_alias, columns in self.input_df_alias_to_cols.items()
            if col_name in columns
        ]
        if len(matching_df_aliases) == 1:
            return matching_df_aliases[0]
        elif len(matching_df_aliases) > 1:
            raise ValueError(
                f"Column '{col_name}' is ambiguous. Please specify the dataframe name."
            )
        raise ValueError(f"Could not infer dataframe alias for column '{col_name}'.")

    def trace_col_origin(self, col_id, visited: Optional[set] = None):
        """Recursively trace back the origins of a column."""

        # Circular dependency check
        if visited is None:
            visited = set()
        if col_id in visited:
            logger.error(f"Circular dependency detected for column {col_id}.")
            return []
        visited.add(col_id)
        # End circular dependency check

        if col_id not in self.dict_step:
            logger.warning(f"Warning: Column {col_id} has no documented origin.")
            return []
        step = self.dict_step[col_id]

        used_steps = [step]
        for input_col_id in step.input_cols:
            used_steps.extend(self.trace_col_origin(input_col_id, visited))

        return used_steps

    @property
    def step_list(self) -> List[ColStep]:
        if not self._step_list:
            self._step_list = list(self.dict_step.values())
        return self._step_list

    def get_steps_for_alias(self, alias) -> List[ColStep]:
        """All steps of a given alias"""
        return [step for step in self.step_list if step.alias == alias]

    def get_all_steps(self, steps: List[ColStep]):
        all_steps = []
        for step in steps:
            all_steps += self.trace_col_origin(step.id)
        # drop duplicated
        res = []
        [res.append(x) for x in all_steps if x not in res]
        return res

    def last_steps_for_each_col(self, cols: List[str], alias) -> List[ColStep]:
        """
        Return the last documented step for each column.
        :param cols:
        :param alias:
        :return:
        """
        all_steps = []
        for col in cols:
            steps = self.highest_matching_cdt(
                self._step_list, lambda step: step.col == col and step.alias == alias
            )
            if len(steps) > 1:
                raise ValueError(f"Column '{col}' is ambiguous. Found: {steps}")
            if not steps:
                logger.warning(f"Column '{col}' has no documented origin.")
            else:
                all_steps.append(steps[0])
        return all_steps

    def get_drop_steps(self, df_cols, top_level_steps, alias):
        """Add DROPPED step for columns that are not in the DataFrame."""
        documented_cols: List[str] = [step.col for step in top_level_steps if step.name != DROPPED]
        drop_cols = list(set(documented_cols) - set(df_cols))
        to_be_dropped = [step for step in top_level_steps if step.col in drop_cols]

        drop_steps = []
        for step in to_be_dropped:
            drop_steps.append(
                self.add_step_simplified(
                    alias, step.col, DROPPED, [step], current_step=True, return_=True
                )
            )
        return drop_steps

    def add_created_cols(self, df_cols, top_level_steps, alias):
        """Add CREATED step for columns that are not in the DataFrame."""
        created_cols = list(set(df_cols) - set([step.col for step in top_level_steps]))
        create_steps = []
        for col in created_cols:
            create_steps.append(
                self.add_step_simplified(alias, col, CREATED, [], current_step=True, return_=True)
            )
        return create_steps

    def get_last_level_step_per_col(self, alias, drop_included=False):
        """
        from self.step_list
        get all columns of the given alias
        get all last level steps for each column
        """
        all_steps_alias = self.get_steps_for_alias(alias)
        all_cols_alias = list(set([step.col for step in all_steps_alias]))
        last_level_steps = self.last_steps_for_each_col(all_cols_alias, alias)
        return [step for step in last_level_steps if (drop_included or step.name != DROPPED)]

    def metadata(self, df, alias):
        df_cols = list(df.columns)

        top_level_steps = self.get_last_level_step_per_col(alias, drop_included=True)
        drop_steps = self.get_drop_steps(df_cols, top_level_steps, alias)
        create_steps = self.add_created_cols(df_cols, top_level_steps, alias)

        steps = self.get_all_steps(top_level_steps) + drop_steps + create_steps
        return [step.__dict__() for step in steps]  #  if step.current_step is True]

    def get_origin_from_step(self, step: ColStep) -> List[str]:
        """
        Trace back until in finds a step starting with "origin:" and return the origin info.
        Function recursive as it can traceback before a merge of multiple columns that generated the current one.
        to traceback we use step.input_cols
        :param step:
        :return:
        """
        if step.name.startswith("origin:"):
            return step.name.split("origin:")[1:]
        return [
            i for is_ in step.input_cols for i in self.get_origin_from_step(self.dict_step[is_])
        ]

    def find_advanced_col_step(self, col_name, df_alias: None | str, include_dropped=False) -> None | List[ColStep] | ColStep:
        """
        Return the documented step that generated a column.
        :param col_name:
        :param df_alias:
        :param include_dropped: if True, include dropped steps in the documentation
        :return:
        """
        df_alias = df_alias or self._infer_dataframe_alias(col_name)

        matching_steps = self.highest_matching_cdt(
            self.step_list,
            lambda step: step.col == col_name
            and step.alias == df_alias
            and (include_dropped or step.name != DROPPED),
        )

        if not matching_steps:
            logger.warning(f"Column '{col_name}' has no documented origin.")
            return None

        if len(matching_steps) > 1:
            logger.error(f"Column '{col_name}' is ambiguous. Found: {matching_steps}")
            return matching_steps

        return matching_steps[0]

    def get_origin(self, col_name, df_alias) -> List[str]:
        """
        Return the origin of a column.
        :param col_name:
        :param df_alias:
        :return:
        """
        step = self.find_advanced_col_step(col_name, df_alias)
        return self.get_origin_from_step(step)

    def get_documentation_from_step(self, step: ColStep) -> List[str]:
        """
        Trace back until in finds a step starting with "origin:" and return the origin info.
        Function recursive as it can traceback before a merge of multiple columns that generated the current one.
        to traceback we use step.input_cols
        :param step:
        :return:
        """
        if len(step.input_cols) == 1:
            return [
                i
                for is_ in step.input_cols
                for i in self.get_documentation_from_step(self.dict_step[is_])
            ] + [step.name]
        return [
            self.get_documentation_from_step(self.dict_step[is_]) for is_ in step.input_cols
        ] + [step.name]

    def get_documentation(self, col_name, df_alias=None, include_dropped=False) -> List[str]:
        """
        Return the documentation of a column. (meaning those all the steps that generated the column)
        :param col_name:
        :param df_alias:
        :param include_dropped: if True, include dropped steps in the documentation
        :return:
        """
        step = self.find_advanced_col_step(col_name, df_alias, include_dropped)
        if isinstance(step, list):
            raise ValueError(f"Column '{col_name}' is ambiguous. Found: {step}")
        return self.get_documentation_from_step(step) if step else []

    def highest_matching_cdt(self, pool: List[ColStep], cdt) -> List[ColStep]:
        # Step 1: Create a set of marked steps
        marked = set()

        # Step 2: Use the current direction of the graph to create a mapping from each node to its ancestors (input_cols)
        mapping = {step.id: step.input_cols for step in pool}

        # Step 3: Traverse the pool and mark matching steps and their descendants
        def mark_parents(step_id):
            for parent_id in mapping[step_id]:
                if parent_id in marked:
                    continue
                marked.add(parent_id)
                mark_parents(parent_id)

        for step in pool:
            if cdt(step):
                mark_parents(step.id)

        # Step 4: Filter the pool for steps matching the condition but don't have marked ancestors
        result = [step for step in pool if cdt(step) and step.id not in marked]

        return result

    # def _has_ancestor_matching_cdt(self, step: ColStep, cdt, memo: Dict[str, bool]) -> bool:
    #     """
    #     Return True if the step or any of its ancestors match the condition, otherwise return False.
    #     """
    #     # If the result is already computed, return it
    #     if step.step_id in memo:
    #         return memo[step.step_id]
    #
    #     if cdt(step):
    #         memo[step.step_id] = True
    #         return True
    #
    #     for input_col in step.input_cols:
    #         if self._has_ancestor_matching_cdt(self.dict_step[input_col], cdt, memo):
    #             memo[step.step_id] = True
    #             return True
    #
    #     # Store the result in the memo table
    #     memo[step.step_id] = False
    #     return False
    #
    # def highest_matching_cdt(self, pool: List[ColStep], cdt) -> List[ColStep]:
    #     """
    #     Return steps in pool that match cdt and do not have any ancestors matching cdt.
    #     """
    #     result = []
    #     memo = {}
    #     for step in pool:
    #         if cdt(step) and not any(
    #             self._has_ancestor_matching_cdt(self.dict_step[input_col], cdt, memo)
    #             for input_col in step.input_cols
    #         ):
    #             result.append(step)
    #     return result

    # def _has_descendant_matching_cdt(self, step: ColStep, cdt, memo: Dict[str, bool]) -> bool:
    #     """
    #     Return True if the step or any of its descendants match the condition, otherwise return False.
    #     """
    #     # If the result is already computed, return it
    #     if step.id in memo:
    #         return memo[step.id]
    #
    #     if cdt(step):
    #         memo[step.id] = True
    #         return True
    #
    #     for input_col in step.input_cols:
    #         print(f"going through input cols of step {step}")
    #         if self._has_descendant_matching_cdt(self.dict_step[input_col], cdt, memo):
    #             memo[step.id] = True
    #             return True
    #
    #     # Store the result in the memo table
    #     memo[step.id] = False
    #     return False
    #
    # def deepest_matching_cdt(self, pool: List[ColStep], cdt) -> List[ColStep]:
    #     """
    #     Return steps in pool that match cdt and do not have any descendants matching cdt.
    #     """
    #     result = []
    #     memo = {}
    #     for step in pool:
    #         print(step, cdt(step))
    #         if cdt(step) and not any(
    #             self._has_descendant_matching_cdt(self.dict_step[input_col], cdt, memo)
    #             for input_col in step.input_cols
    #         ):
    #             result.append(step)
    #     return result

    # def deepest_matching_cdt(self, pool: List[ColStep], cdt) -> List[ColStep]:
    #     # Step 1: Create a set of marked steps
    #     marked = set()
    #
    #     # Step 2: Create a reverse mapping
    #     reverse_mapping = {step.id: [] for step in pool}
    #     for step in pool:
    #         for input_col in step.input_cols:
    #             reverse_mapping[input_col].append(step.id)
    #
    #     # Step 3: Traverse the pool and mark matching steps and their ancestors
    #     def mark_ancestors(step_id):
    #         if step_id in marked:
    #             return
    #         marked.add(step_id)
    #         for child_id in reverse_mapping[step_id]:
    #             mark_ancestors(child_id)
    #
    #     for step in pool:
    #         if cdt(step):
    #             mark_ancestors(step.id)
    #
    #     # Step 4: Filter the pool for steps matching the condition but don't have marked descendants
    #     for step in pool:
    #         print(step, cdt(step), all(child_id not in marked for child_id in reverse_mapping[step.id]))
    #     result = [
    #         step
    #         for step in pool
    #         if cdt(step) and all(child_id not in marked for child_id in reverse_mapping[step.id])
    #     ]
    #
    #     return result
