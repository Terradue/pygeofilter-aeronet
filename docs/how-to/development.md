# Set up development and preview documentation

Clone the repository and install an editable copy using Python 3.10–3.13:

```bash
git clone https://github.com/Terradue/pygeofilter-aeronet.git
cd pygeofilter-aeronet
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

## Preview the documentation

With Task and uv installed, run:

```bash
task serve-docs
```

Open the local URL printed by MkDocs. Notebook pages render their saved outputs; to refresh those outputs, run the notebook explicitly in an environment with the package, JupyterLab, and Folium installed.

## Place a documentation change

Use the [Diátaxis framework](https://diataxis.fr/) to choose the page's purpose:

- **Tutorials** guide a learner through a complete exercise with a visible outcome.
- **How-to guides** give steps for a specific task and state prerequisites.
- **Reference** records interfaces, accepted values, defaults, and limitations.
- **Explanation** describes concepts, design choices, and their consequences.

Link to other sections when readers need a different kind of help. Add pages to `mkdocs.yaml` and keep existing notebook paths stable so published links continue to work.
