VERSION=3.10.5
pyenv install "$VERSION"
pyenv local "$VERSION"
poetry init
poetry env use "$VERSION"
poetry install
