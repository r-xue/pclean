# Updating `defaults.yaml`

`defaults.yaml` is **auto-generated** from the pydantic field defaults
in `config.py` and **committed to the repo**.  It serves as a
human-readable reference that users can browse on GitHub without
running Python.

## Workflow

1. Change the default value in `src/pclean/config.py`.
2. Regenerate the snapshot:
   ```bash
   pixi run -e dev gen-defaults
   ```
3. Commit both `config.py` and `defaults.yaml` together.

## CI Guard (optional)

Add a step that regenerates the file and asserts no diff, catching
cases where step 2 was forgotten:

```yaml
- name: Check defaults.yaml is up to date
  run: |
    pixi run -e dev gen-defaults
    git diff --exit-code src/pclean/configs/defaults.yaml
```
