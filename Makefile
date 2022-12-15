build: pip
	printenv GCP_KEYFILE > /home/vscode/keyfile.json
pip:
	pip install --upgrade pip wheel