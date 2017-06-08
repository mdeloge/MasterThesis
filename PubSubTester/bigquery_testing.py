from google.cloud import bigquery

client = bigquery.Client(project="lkn-muntstraat")
dataset = client.dataset("Jouleboulevard")

if dataset.exists():
    print "dataset exists!"
    table = dataset.table("EnergieID_15min")

