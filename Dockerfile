FROM python:3.6

RUN mkdir -p /alameda-ai/
WORKDIR /alameda-ai/

ENV PYTHONPATH /alameda-ai/

RUN apt update && apt install -y vim git

COPY framework /alameda-ai/framework
COPY services /alameda-ai/services
COPY requirements.txt requirements.txt

# Service required python packages:
RUN pip install -r requirements.txt

# Additional python packages (includes framework):
RUN pip install filelock

# Ports to be exposed:
EXPOSE 50051/tcp

ENV OPERATOR_ADDRESS=${OPERATOR_ADDRESS:-operator.alameda.svc.cluster.local:50050}

CMD ["python3", "-B", "/alameda-ai/services/arima/main.py"]
