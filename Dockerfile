from alpine

run apk add python3 py3-pip python3-dev gcc musl-dev libheif-dev

copy requirements.txt .
copy main.py .

run pip3 install wheel
run pip3 install -r requirements.txt

cmd ["python3", "main.py"]
