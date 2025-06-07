FROM ubuntu:24.04

# Set non-interactive frontend for apt to avoid tzdata issues
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies and tools
RUN apt-get update && apt-get install -y \
    curl \
    git \
    zsh \
    unzip \
    ca-certificates \
    build-essential \
    libssl-dev \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (or use existing ubuntu user)
RUN if ! id -u ubuntu >/dev/null 2>&1; then \
        useradd -m -s /bin/zsh ubuntu; \
    fi && \
    echo 'ubuntu ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Switch to the ubuntu user
USER ubuntu
WORKDIR /home/ubuntu

# Install rustup and nightly toolchain for the ubuntu user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . $HOME/.cargo/env \
    && rustup install nightly \
    && rustup default nightly

# Set environment variables for Rust and local bin
ENV PATH="/home/ubuntu/.local/bin:/home/ubuntu/.cargo/bin:${PATH}"

# Install oh-my-zsh for ubuntu user
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

# Install starship to user's local bin
RUN mkdir -p ~/.local/bin && \
    curl -sS https://starship.rs/install.sh | sh -s -- --bin-dir ~/.local/bin -y

# Add starship to zsh config
RUN echo 'eval "$(starship init zsh)"' >> ~/.zshrc

# Set default shell to zsh
SHELL ["/usr/bin/zsh", "-c"]

# Default command
CMD ["zsh"]
