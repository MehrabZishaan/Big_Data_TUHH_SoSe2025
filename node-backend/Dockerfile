FROM node:lts AS build

WORKDIR /usr/src/app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY server.js server.js

# Expose port
EXPOSE 5000

# Start the application
CMD ["npm", "start"]