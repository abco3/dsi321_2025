FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install streamlit pandas s3fs pyarrow

COPY . /app

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
