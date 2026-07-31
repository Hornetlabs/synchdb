[![Netlify Status](https://api.netlify.com/api/v1/badges/5ead234b-270b-4b31-93e1-ea0e20774739/deploy-status)](https://app.netlify.com/sites/synchdbdocs/deploys)
# SynchDB Documentation

This repository contains the documentation for SynchDB, a PostgreSQL extension for synchronizing data from different database sources. The documentation is built using MkDocs with the Material theme and supports multiple languages.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- Python 3.7 or higher
- pip (Python package installer)

### For macOS users:

If you don't have Python 3 installed, you can install it using Homebrew:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install python
```

### For Ubuntu users:

Python 3 is typically pre-installed on Ubuntu. If it's not, you can install it using:

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/Hornetlabs/synchdb-docs.git
   cd synchdb-docs
   ```

2. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```

3. Activate the virtual environment:
   - On macOS and Ubuntu:
     ```bash
     source venv/bin/activate
     ```

4. Update pip to the latest version:
   ```bash
   pip install --upgrade pip
   ```

5. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

Note: If you see a notice about a new pip version available after running the last command, you can update pip again using the command provided in step 4.

## Running the Documentation Locally

To serve the documentation locally:

1. Ensure you're in the project root directory and your virtual environment is activated.

2. Run the following command:
   ```bash
   mkdocs serve
   ```

3. Open your web browser and navigate to `http://127.0.0.1:8000/`

You should now see the SynchDB documentation website running locally.

## Building the Documentation

To build the static site:

1. Run the following command:
   ```bash
   mkdocs build
   ```

2. The built site will be in the `site` directory.

## Project Structure

```
synchdb-docs/
├── docs/
│   ├── en/
│   │   ├── index.md
│   │   ├── user-guide/
│   │   │   ├── installation.md
│   │   │   ├── configuration.md
│   │   │   └── usage.md
│   │   ├── architecture.md
│   │   ├── api-reference.md
│   │   └── changelog.md
│   ├── es/
│   │   └── ... (same structure as 'en')
│   └── zh/
│       └── ... (same structure as 'en')
├── mkdocs.yml
├── requirements.txt
└── README.md
```

## Adding or Modifying Content

1. To add new pages, create `.md` files in the appropriate language directory under `docs/`.
2. To modify existing content, edit the corresponding `.md` files.
3. Update the `nav` section in `mkdocs.yml` if you add new pages.

## Multi-language Support

This documentation supports English (en), Spanish (es), and Chinese (zh). To add or modify translations:

1. Ensure the same file structure exists in each language directory (`en/`, `es/`, `zh/`).
2. Translate the content in the corresponding `.md` files.
3. Update the `nav_translations` in `mkdocs.yml` if you add new pages.

## Contributing

1. Fork the repository.
2. Create a new branch for your changes.
3. Make your changes and commit them with clear, concise commit messages.
4. Push your changes to your fork.
5. Submit a pull request with a description of your changes.

## Support

If you encounter any issues or have questions, please open an issue in this repository.

Note: If you encounter any pip-related issues, first try updating pip to the latest version:
```bash
pip install --upgrade pip
```
Then, re-run the installation of the requirements:
```bash
pip install -r requirements.txt
```
If the issue persists, please include the output of `pip --version` when opening an issue.

### Troubleshooting

- If you encounter permission issues on Ubuntu when installing packages globally, you may need to use `sudo` or consider using a virtual environment (as recommended in the setup instructions).
- On macOS, if you have issues with Homebrew, ensure your system is up to date and try running `brew doctor` to diagnose and fix common issues.
