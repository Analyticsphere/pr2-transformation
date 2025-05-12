"""
Provides utilities for rendering SQL SELECT expressions from configurable
transformation templates.

This module is designed to support flexible one-off data transformations
using parameterized SQL templates. It supports:

- One-to-one, many-to-one (coalesce), and one-to-many (split) mappings
- Indexed placeholders such as {source[0]} or {target[1]} for list-based inputs
- Clean formatting with automatic trailing comma removal and line splitting

Used in conjunction with a dictionary of custom transformation entries
(e.g., in `constants.py`) to dynamically build SQL statements.
"""

class FormatDict(dict):
  """
  Custom dictionary to support indexed placeholders like {source[0]} and {target[1]}.
  """
  def __getitem__(self, key):
      if key.startswith("source["):
          idx = int(key[len("source["):-1])
          return self['source'][idx]
      elif key.startswith("target["):
          idx = int(key[len("target["):-1])
          return self['target'][idx]
      else:
          return super().__getitem__(key)

class SQLTransformRenderer:
  @staticmethod
  def render(entry: dict) -> list[str]:
      """
      Renders SQL select expressions from a custom transformation entry.

      Supports:
      - One-to-one: a single source maps to a single target
      - Many-to-one: multiple sources are coalesced into one target
      - One-to-many: a single source is split into multiple targets
      - Indexed formatting: allows {source[0]}, {target[1]}, etc. in the SQL template

      Args:
          entry (dict): A transformation entry with keys:
              - source (str or list[str]): Input column(s)
              - target (str or list[str]): Output column(s)
              - transform_template (str): SQL snippet using Python format syntax

      Returns:
          list[str]: Rendered SQL expression(s), one per SELECT clause line

      Example:
      --------
      >>> entry = {
      ...     "source": ["D_123456789", "D_987654321"],
      ...     "target": "D_123456789_coalesced",
      ...     "transform_template": "COALESCE({source[0]}, {source[1]}) AS {target},"
      ... }
      >>> SQLTransformRenderer.render(entry)
      ['COALESCE(D_123456789, D_987654321) AS D_123456789_coalesced']
      """
      source = entry["source"]
      target = entry["target"]
      template = entry["transform_template"]

      source_list = source if isinstance(source, list) else [source]
      target_list = target if isinstance(target, list) else [target]

      context = FormatDict({
          "source": source_list,
          "target": target_list
      })

      rendered = template.format_map(context).strip().rstrip(',')
      return [line.strip().rstrip(',') for line in rendered.splitlines() if line.strip()]