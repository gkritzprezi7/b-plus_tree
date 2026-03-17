# 1. Use an older Ubuntu base image to ensure Docker Scout finds OS-level vulnerabilities
FROM ubuntu:24.04

# 2. Prevent interactive prompts during installation (like time zone selections)
ENV DEBIAN_FRONTEND=noninteractive

# 3. Install GCC, Make, and standard C libraries
RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

# 4. Set the working directory inside the container
WORKDIR /usr/src/app

# 5. Copy all your project files (src, include, Makefile, etc.) into the container
COPY . .

# 6. Compile the B+ Tree implementation using your existing Makefile
RUN make bplus_main_compile

CMD ["make bplus_main_run"]