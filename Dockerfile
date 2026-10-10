FROM apache/airflow:2.8.1-python3.11

USER root

# Tải Java 17 Temurin OpenJDK trực tiếp (Bỏ qua apt-get mirror)
RUN mkdir -p /opt/java && \
    curl -sL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.10%2B7/OpenJDK17U-jdk_aarch64_linux_hotspot_17.0.10_7.tar.gz" \
    | tar -xz -C /opt/java --strip-components=1

# Cấu hình biến môi trường JAVA_HOME
ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Cài đặt Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt