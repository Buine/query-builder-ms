const express = require('express')
const { createQuery } = require('../services/queryBuilderService')

const queryRouter = express.Router()

queryRouter.post('/query', (req, res) => {
    const rawSql = createQuery(req.body)
    res.send({
      integration_code: req.body.integration_code,
      sql_query: rawSql 
    })
  })

module.exports = queryRouter