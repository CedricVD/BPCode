const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');

const projectId = 'bachelorproef-275417';   
const dataset = 'GARawData';
const tableName = 'RawData';
const bucketName = 'garawdatabucket';

const bigQuery = new BigQuery({
    projectId: projectId
});

const storage = new Storage({
    projectId: projectId
}).bucket(bucketName);


exports.GA_Raw_Hits = (req, res) => {
    try {
        res.set('Access-Control-Allow-Origin', '*');
        if (req.method === 'OPTIONS') {
            // Send response to OPTIONS requests
            res.set('Access-Control-Allow-Methods', 'POST');
            res.set('Access-Control-Allow-Headers', 'Content-Type');
            res.set('Access-Control-Max-Age', '3600');
            res.status(204).send('');
        }
        
        if(req.body!==null){
            var hit = req.body;
            hit['hit_timestamp'] = new Date().getTime();
            hit.payload=decodeURIComponent(hit.payload)
            
            bigQuery
            .dataset(dataset)
            .table(tableName)
            .insert(hit)
            .then((data) => console.log("data inserted to big query ", data))
            .catch(err => {
                console.log("Error: can't insert ",err);
                createFile(err, hit);
            })
        }
    }
    catch (e) {
        console.log('inside catch with error: ',e)
        createFile(e, req.body);
    } 
    res.status(200).send('acknowledged');
};

//store hit in cloud bucket.
function createFile(error,entities) {
    var isotime = new Date().toISOString()+"__"+Math.round(Math.random()*10000); 
    var filename = 'ga_raw_hits_errors/'+error+'/'+error+'_'+isotime+'.ndjson';
    
    // also adding filename to the raw hit will help to backtrack.
    entities['filename'] = filename;
    
    var gcsStream = storage.file(filename).createWriteStream();
        var str = JSON.stringify( entities );
        gcsStream.write( str + '\n' );
        
    gcsStream.on('error', (err) => {
        console.error(`${ this.archive }: Error storage file write.`);
        console.error(`${ this.archive }: ${JSON.stringify(err)}`);
    });
    gcsStream.end();
}