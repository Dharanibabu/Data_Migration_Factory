# Creating Sphinx Documentation Guide

## 1. Move to parent DMF/ directory:

```bash
cd DMF/
```

## 2. Create a docs directory:
```bash
mkdir docs
```

## 3. Move to docs directory:
```bash
cd docs
```

## 4. Run sphinx-quickstart to give basic details.
  (for the first question, use default.)

## 5. Update docs/conf.py:
### a. Add this at the top of the file:
```python
import os, sys
sys.path.insert(0, os.path.abspath(".."))
```
### b. Update the empty extensions list:
```python
extensions = [
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.autodoc"
]
```
### Optionally, update the HTML theme (default: "sphinx_rtd_theme").

## 6. Move to DMF/ directory:
```bash
cd DMF
```

## 7. Generate .rst files for all packages:
```bash
sphinx-apidoc -o docs dmf/
```

## 8. Move to docs directory and run make html command.

After this step, docs/_build/html/index.html will serve as the main page of the documentation.

**Note:**
Once the above changes are implemented, avoid repeating all the steps.
Delete the `docs/_build` directory and regenerate the documentation using the `make html` command.
