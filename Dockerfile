FROM node:20-alpine
WORKDIR /app
COPY package.json ./
COPY index.js ./
COPY service ./service
ENV NODE_ENV=production
EXPOSE 3000
CMD ["node", "index.js"]
