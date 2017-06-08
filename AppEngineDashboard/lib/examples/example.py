import os
import sqlite3
from string import Template

from gviz_data_table import Table, encoder


def data():
    folder = os.path.split(__file__)[0]
    db = sqlite3.connect(os.path.join(folder, "sample.db"))
    c = db.cursor()
    c.execute("SELECT name, salary FROM employees")
    cols = [dict(id=col[0], label=col[0].capitalize(), type=col[1])
            for col in c.description]
    # sqlite3 unfortunately does not provide type information
    cols[0]['type'] = unicode
    cols[1]['type'] = float

    t = Table(cols)
    for r in c.fetchall():
        name, value = r
        label = "${0}".format(value)
        t.append([name, (value, label)])

    return encoder.encode(t)


template = Template("""
<html>

<head>
<script src="http://www.google.com/jsapi" type="text/javascript"></script>
</head>

<body>
<script>
      google.load("visualization", "1", {packages:["corechart", "table"]});

      var data = $data

      google.setOnLoadCallback(drawChart);
      function drawChart() {

      var chart_data = new google.visualization.DataTable(data);
      var chart = new google.visualization.ColumnChart(document.getElementById('chart'));
      chart.draw(chart_data);
      }

      google.setOnLoadCallback(drawTable);
      function drawTable () {
      var table_data = new google.visualization.DataTable(data);
      var table = new google.visualization.Table(document.getElementById('table'));
      table.draw(table_data);
      }
</script>

<div id="chart"></div>
<div id="table"></div>

</body>

</html>
""")

def save():
    with open("result.html", "wb") as f:
        f.write(template.safe_substitute(data=data()))

if __name__ == "__main__":
    save()