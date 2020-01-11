const fs = require('fs')
const cheerio = require("cheerio");
const Axios = require('axios')
const query = require('./query')

const { connect } = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'data_gap_analyzer';

const new_db = 'test_db';
const  new_col = 'account_population';

//2852
connect(url, { useUnifiedTopology: true }, function(err, client) {
    console.log("Connected successfully to server");
    const collection = client.db(dbName).collection('site_info');
    const site_id = "2852";
    getSumInfoList(collection, site_id).then((sum_infos) => {
        console.log("Sum Info Fetched: " + JSON.stringify(sum_infos));
        generateQuery(sum_infos).then(queries => {
            console.log("Inside Query " + queries.length);
            // login to splunk
            getToken().then(token => {
                console.log('Token: ' + token);
                queries.forEach(query => {
                    getSid(token, query).then(sid => {
                        console.log("SID : " + sid);
                        getResult(sid, token).then(result => {
                            let data = JSON.stringify(result).toUpperCase()
                            data = JSON.parse(data);
                            data.map(data => (
                                data['SITE_ID'] = site_id
                            ));
                            const col = client.db(new_db).collection(new_col);
                            col.insertMany(data).then(res => {
                                console.log("Data Inserted")
                            }).catch(err => {
                                console.log("Error: " + err)
                            })
                        }).catch(err => {
                            console.error("Query ERROR : " + err)
                        })
                    }).catch(err => {
                        console.error("SID ERROR : " + err)
                    })
                })
            })
        })
    }).catch((err) => {
        console.log(err)
    });
});

const login = () => {
    const url = 'https://splunkapi.yodlee.com/services/auth/login';
    const body = {
        username: "spaudel",
        password: encodeURIComponent("qazxsw@13")
    };

    return Axios.post(url, `username=${body.username}&password=${body.password}`, {
        headers: {'Content-Type': 'application/x-www-form-urlencoded'}
    })
};

const getSumInfoList = (collection, site_id) => {
    return new Promise((resolve, reject) => {
        collection.find({SITE_ID: site_id}, {projection: {_id: 0}}).toArray().then(data => {
            resolve(data)
        }).catch(err => {
            reject(err)
        })
    })
};

const generateQuery = (sum_infos) => {
    let queries = [];
    return new Promise((resolve, reject) => {
        sum_infos.forEach(sum_info => {
            let si = sum_info['SUM_INFO_ID'];
            let con = sum_info['TAG'];

            if (con === "bank") {
                let text = query.bankAccountQuery;
                text = text.replace("#", si);
                queries.push(text)
            } else if (con === "credits") {
                let text = query.cardAccountQuery;
                text = text.replace("#", si);
                queries.push(text)
            } else if (con === "loans") {
                let text = query.loanAccountQuery;
                text = text.replace("#", si);
                queries.push(text)
            } else if (con === "stocks") {
                let text = query.investmentAccountQuery;
                text = text.replace("#", si);
                queries.push(text)
            } else if (con === "insurance") {
                let text = query.insuranceAccountQuery;
                text = text.replace("#", si);
                queries.push(text)
            }
        });
        resolve(queries)
    })
};

const getToken = () => {
    return new Promise((resolve, reject) => {
        login().then(async res => {
            const $ = await cheerio.load(res.data, {
                xmlMode: true
            });
            const data = await $('sessionKey')[0].children[0].data;
            resolve(data)
        }).catch(err => reject(err))
    })
};

const getSid = (token, query) => {
    const url = 'https://splunkapi.yodlee.com/services/search/jobs?output_mode=json';
    query = encodeURIComponent(query);
    return new Promise((resolve, reject) => {
        Axios.post(url, `search=${query}`, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': `Splunk ${token}`
            }
        }).then(res => {
            const sid = res.data['sid'];
            getJobStatus(sid, token, 0).then(done => {
                console.log("Inside Resolved");
                resolve(sid);
            }).catch(err => {
                console.log("Inside Error")
                reject(err);
            })
        }).catch(err => {
            reject(err.response.data);
        })
    })
};

const getResult = (sid, token) => {
    const url = `https://splunkapi.yodlee.com/services/search/jobs/${sid}/results?output_mode=json&offset=0&count=50000`;
    return new Promise((resolve, reject) => {
        Axios.get(url,{
            headers: {
                'Authorization': `Splunk ${token}`
            }
        }).then(res => {
            console.log(res.status)
            resolve(res.data.results);
        }).catch(err => {
            console.log("Fail: " + err)
           reject(err);
        })
    })
};

const getJobStatus = (sid, token, count) => {
    console.log("Count: " + count);

    const url = `https://splunkapi.yodlee.com/services/search/jobs/${sid}`;
    return new Promise((resolve, reject) => {
        Axios.get(url, {
            headers: {
                'Authorization': `Splunk ${token}`
            }
        }).then(res => {
            let data = res.data;
            let done = data.substring(data.indexOf("<s:key name=\"isDone\">") + 21, data.indexOf("<s:key name=\"isDone\">") + 22);
            let status = 0;
            console.log("Is Done: " + done);
            if (Number(done) === 1) {
                status = data.substring(data.indexOf("<s:key name=\"isFailed\">") + 23, data.indexOf("<s:key name=\"isFailed\">") + 24);
                resolve(sid);
            } else {
                if (count > 40) {
                   reject(null);
                }
                setTimeout(() => {
                    status = getJobStatus(sid, token, ++count)
                    resolve(status);
                }, 3000);
            }
        }).catch(err => {
            console.log(err)
            reject("failed")
        })
    })
};
