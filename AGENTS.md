## Code Style & Convention Guidelines
- Prefer expressive, descriptive names over abbreviations. Avoid vague function names like `process_data()`. Names can be as long as needed to be self documenting.
- Keep helper utilities private with a leading `_` when they are not meant to be exported from a module or file.
- Always type every function parameter and return value. Use `pydantic` models or dataclasses for structured data—never bare dictionaries with fixed keys.
- Let errors/exceptions bubble up; only catch errors when the failure is an expected control path, to re raise with additional context, or if you are at the top level of a request handler and need to log the failure.
- Do not try to gracefully handle data being unexpected null; let it fail loudly so the issue can be addressed upstream.
  ```python
  # Bad Example:
  def calculate_area(geometry: Optional[Geometry]) -> float:
      if geometry is None:
          return 0.0

      if not hasattr(geometry, 'area'):
          geometry.area = 0.0
          return 0.0

      return geometry.area

  # Better Example:
  def calculate_area(geometry: Geometry) -> float:
      return geometry.area
  ```

- Avoid Optional (or ` | None`) types in parameters and class attributes unless absolutely needed. It is better to make a child class or separate function so that it is always clear what values are populated when reading the code and so that you do not need to litter the code with null checks.
  ```python
  # Bad Example:
  class Person:
      name: str
      age: int
      drivers_license_number: Optional[str]
      drivers_license_expiration: Optional[date]
  # Better Example:
  class Person:
      name: str
      age: int

  class LicensedPerson(Person):
      drivers_license_number: str
      drivers_license_expiration: date
  ```

## Project Context & Workflow
- This project is a Textual-based TUI for Postgres, inspired by k9s and DBeaver.
- The UI uses async data loading via `asyncpg`; keep network calls async and avoid blocking the event loop.
- Prefer short-lived, read-only connections; use `default_transaction_read_only` and timeouts as already configured.
- Keep view state explicit and avoid hiding behavior in implicit globals or side effects.
- Keybindings should be context-aware and match k9s-style expectations where possible.
