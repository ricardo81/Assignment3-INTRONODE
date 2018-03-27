const mongodb= require('mongodb')
const url = 'mongodb://localhost:27017/edx-course-db'
const customers = require('./m3-customer-data.json')
const customerAddresses = require('./m3-customer-address-data.json')
const async = require('async')

let tasks = []
const step = parseInt(process.argv[2])

mongodb.MongoClient.connect(url, (error, db) => {
  if (error) return process.exit(1)

  var dbo = db.db("mydb");
  dbo.createCollection("customers", function(err, res) {
    if (err) throw err;
  });

  customers.forEach((customer, index, list) => {
    customers[index] = Object.assign(customer, customerAddresses[index])
    
    if (index % step == 0) {
      const start = index
      const end = (start+step > customers.length) ? customers.length-1 : start+step
      tasks.push((done) => {
        dbo.collection('customers').insert(customers.slice(start, end), (err, results) => {
          done(err, results)
        })
      })
    } 
  })
  
  async.parallel(tasks, (err, results) => {
    if (err) console.error(err)
    console.log('Inserted records into DataBase '+step+' at a time.')
    db.close()
  })

})