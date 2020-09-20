<h1>Project: Data Warehouse</h1>
<h2>Company: Sparkify</h2>
<h3>By: Salinee Kingbaisomboon</h3>

<hr />

<b>Purpose of this database:</b>
The purpose of database is to keep data systematic so that it can be easily managed, accessed and update. We decide to convert all the data reside in files to the <b>RDBM</b> for ease of analytics's purposes.

<b>Analytical goals:</b>
<p>This collection of data and information can be used by many stakeholders. For example,[1]
     <ul>
        <li><b>Customer-Facing team (such as marketing):</b> marketing team can use this database to generate personalized communications in order to promote a product or service for marketing purposes.</li>
        <li><b>Executive team (such as CEO):</b> They need to use data to run and grow their business. The goal is to help them make better decisions, which can take them to the top of their market.</li>
        <li><b>Data Scientist team:</b> the main goal of this team is to extracting business-focused insights from data. For most organizations, data science is employed to transform data into value in the form improved revenue, reduced costs, business agility, improved customer experience, the development of new products, and the like.</li>
     </ul>
</p>

<b>State and justify the database schema and ETL pipeline:</b>
<ul>
    <li>
        Database Schema:
        <p>The intrsuction stated that we need to create a star schema optimized for queries on song play analysis.</p>
        <ul>
            <li>Fact Table: <b>songplays</b> - records in log data associated with song plays with only <i><b>page = NextSong</b>.</i> This table will provides the metric of the business process.</li>
            <li>Dimension Tables: The following tables can answer all questions related to <i><b>Where</b></i>, <i><b>When</b></i>, <i><b>What</b></i>.
                <ol>
                    <li><b>users</b> - users in the app</li>
                    <li><b>songs</b> -  songs in music database</li>
                    <li><b>artists</b> - artists in music database</li>
                    <li><b>time</b> - timestamps of records in <b>songplays</b> broken down into specific units</li>
                </ol>
            </li>
        </ul>
    </li>
    <li>
        ETL Pipeline:
        <ol>
            <li>Use <b>IaC</b> concept from <b>L3 Exercise 2 - IaC's exercise</b> to create and lauch my Redshift's cluster. Below is my settings:
                <div><b>End Point</b>: sparkifycluster.ca7la8xpayfr.us-west-2.redshift.amazonaws.com</div>
                <div><b>ARN Role</b>: arn:aws:iam::591775008419:role/dwhRole</div>
                <div><span style="color:red;">Note! I already delete my cluster as instruct so I'm not sure how the instructor will grade this project?</span></div>
            </li>
            <li>Run <i>create_tables.py</i> in a console to create all the tables on sparkifydb database (on the Redshift cluster) which already created from the previous step.</li>
            <li>Run <i>etl.py</i> to simulate automate processes to copy all the records from <b>S3(udacity-dend)</b> to our <i><b>staging tables</b></i>. After that, on the same file, there is a next batch command to insert the records from the <i><b>staging tables</b></i> into our <i><b>fact and dimension tables</b></i>.</li>
        </ol>
    </li>

</ul>

<b>Below is the simple statistic on each tables on AWS Redshift:</b>
<table>
  <tr>
    <th>Table</th>
    <th>Record Counts</th>
    <th>Preview Data on AWS Redshift</th>
  </tr>
  <tr>
    <td>staging_events</td>
    <td>8,056</td>
    <td><img src="preview_data/staging_events - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>staging_songs</td>
    <td>14,896</td>
    <td><img src="preview_data/staging_songs - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>songplays</td>
    <td>320</td>
    <td><img src="preview_data/songplays - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>artists</td>
    <td>10,025</td>
    <td><img src="preview_data/artists - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>songs</td>
    <td>14,896</td>
    <td><img src="preview_data/songs - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>time</td>
    <td>8,023</td>
    <td><img src="preview_data/time - preview data on Redshift.png"></td>
  </tr>
  <tr>
    <td>users</td>
    <td>104</td>
    <td><img src="preview_data/users - preview data on Redshift.png"></td>
  </tr>
</table>

<h4>Reference: </h4>
<ol>
    <li>https://www.analyticsvidhya.com/blog/2019/08/data-science-leader-guide-managing-stakeholders/#:~:text=The%20three%20stakeholders%20are%3A,The%20data%20science%20team</li>
</ol>