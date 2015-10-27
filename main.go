package main

import (
	// "encoding/json"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
	"net/http"
	// "os"
)

const (
	scope  = bigquery.BigqueryScope
	layout = "2006-01-02T15:04:05Z07:00"
	unix   = "1136239445"
)

var (
	jsonFile        = flag.String("creds", "", "A path to your JSON key file for BigQuery, not needed on Compute Engine instances.")
	bqSourceDataset = flag.String("bq-source-dataset", "", "The dataset to dump the visits into")
	bqSourceTable   = flag.String("bq-source-table", "", "The table for the visits")
	bqSourceProject = flag.String("bq-source-project", "", "The table for the visits")

	bqDestDataset = flag.String("bq-dest-dataset", "", "The dataset to dump the visits into")
	bqDestTable   = flag.String("bq-dest-table", "", "The table for the visits")
	bqDestProject = flag.String("bq-dest-project", "", "The table for the visits")

	debugPtr = flag.Bool("debug", false, "Enable debug")
)

func main() {
	flag.Parse()
	// setEnvVars()

	client, err := GoogleClient()

	if err == nil {
		bq, _ := bigquery.New(client)
		dsr := new(bigquery.DatasetReference)
		dsr.DatasetId = *bqSourceDataset
		dsr.ProjectId = *bqSourceProject

		// request := new(bigquery.QueryRequest)
		// request.DefaultDataset = dsr
		// request.Query = "SELECT count(*) FROM []"

		// call := bq.Jobs.Query("", request)

		// resp, err := call.Do()

		// jobs := new(bigquery.JobsService)
		// job := jobs.Query("sapient-catbird-547", request)
		// resp, err := job.Do()
		// fmt.Print(resp.CacheHit, resp.JobReference, err)

		// jobId := resp.JobReference.JobId

		// s, _ := bq.Jobs.GetQueryResults("", jobId).Do()

		// buf, _ := json.Marshal(s)
		// fmt.Println(s, string(buf), "\n\n\n")

		tabr := new(bigquery.TableReference)
		tabr.DatasetId = *bqDestDataset
		tabr.ProjectId = *bqDestProject
		tabr.TableId = "temp_grouped_v2"

		// jcq := new(bigquery.JobConfigurationQuery)
		// jcq.DestinationTable = tabr
		// jcq.Priority = "BATCH"
		// jcq.WriteDisposition = "WRITE_TRUNCATE"
		// jcq.Query = "SELECT ap_mac, COUNT(DISTINCT(client_mac)), DATE(TIMESTAMP(first_seen)) date FROM [dev_sense_v1.sensev4_ct] GROUP BY ap_mac, date"

		// jc := new(bigquery.JobConfiguration)
		// jc.Query = jcq

		// job := new(bigquery.Job)
		// job.Configuration = jc

		// aa, err := bq.Jobs.Insert(*bqSourceProject, job).Do()
		// if err == nil {
		// 	fmt.Print(aa.Id)
		// } else {
		// 	fmt.Print(err)
		// }

		jce := new(bigquery.JobConfigurationExtract)
		jce.DestinationFormat = "csv"
		jce.DestinationUri = "gs://ct_temp/151028.csv"
		jce.SourceTable = tabr

		extractJc := new(bigquery.JobConfiguration)
		extractJc.Extract = jce
		extractJob := new(bigquery.Job)
		extractJob.Configuration = extractJc

		aa, err := bq.Jobs.Insert(*bqSourceProject, extractJob).Do()
		if err == nil {
			fmt.Print(aa.Id)
		} else {
			fmt.Print(err)
		}

	}

}

type GoogleSession struct {
	*http.Client
}

func GoogleClient() (*http.Client, error) {

	client, err := google.DefaultClient(context.Background(), scope)
	if err != nil {
		fmt.Printf("Unable to get default client: %v", err)
		return nil, err
	} else {
		return client, nil
	}

}

// func setEnvVars() {
// 	if *jsonFile != "" {
// 		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *jsonFile)
// 	}
// 	if *debugPtr {
// 		os.Setenv("DEBUG", "true")
// 	}
// }
