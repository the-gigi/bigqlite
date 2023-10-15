$VERSION = "3.11.4"
pyenv install $VERSION
pyenv local $VERSION
poetry init
poetry env use "$VERSION"
poetry install