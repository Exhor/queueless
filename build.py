import os

os.system("poetry update")
os.system("poetry export -f requirements.txt > requirements.txt")
os.system("docker build -t qless .")
