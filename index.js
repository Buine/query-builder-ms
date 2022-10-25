const express = require('express')
const bodyParser = require("body-parser");
const queryRouter = require('./src/routes/queryRouter');
const app = express()
const port = 8080

app.use(bodyParser.urlencoded({extended:false}))
app.use(bodyParser.json())

queryBuilder = express.Router()

// Health Check
queryBuilder.get('/health-check', (req, res) => {
  res.send({status: 'OK'})
})

// Routers
queryBuilder.use('/v1', queryRouter)

// Base Path
app.use('/query-builder-ms', queryBuilder)

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})