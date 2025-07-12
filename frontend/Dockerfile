
FROM node:lts AS build

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci

COPY index.html index.html
COPY vite.config.js vite.config.js
COPY postcss.config.js postcss.config.js
COPY tailwind.config.js tailwind.config.js
COPY public/ public
COPY src/ src

RUN npx update-browserslist-db@latest

RUN npm run build

FROM nginx:alpine
COPY --from=build /app/build/ /usr/share/nginx/html

# use our own configuration
RUN rm -r /etc/nginx/conf.d
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 80