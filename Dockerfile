FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 6002

# 백엔드 API 실행
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "6002"]
