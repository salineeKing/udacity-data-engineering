<h1>Project: Data Modeling with Postgres</h1>
<h2>Company: Sparkify</h2>
<h3>By: Salinee Kingbaisomboon</h3>

<hr />

<b>Purpose of this database:</b>
The purpose of database is to keep data systematic so that it can be easily managed, accessed and update. We decide to convert all the data reside in files to the <b>RDBM</b> for ease of analytics's purposes.

<b>Analytical goals:</b>
<p>This collection of data and information can be used by many stakeholders. For example,
     <ul>
        <li><b>Marketing:</b> marketing team can use this database to generate personalized communications in order to promote a product or service for marketing purposes.</li>
        <li><b>Data Scientist:</b> the main goal of this team is to extracting business-focused insights from data. For most organizations, data science is employed to transform data into value in the form improved revenue, reduced costs, business agility, improved customer experience, the development of new products, and the like.</li>
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
            <li>Run <i>create_tables.py</i> in a console to create sparkifydb database and all the tables.</li>
            <li>Run <i>test.ipynb</i> in a Jupyter notebook to confirm that all the tables are created.</li>
            <li>Run <i>etl.ipynb</i> to develop our ETL processes for each table. (The script here will be use later on the real ETL script file).</li>
            <li>Run <i>etl.py</i> to simulate automate processes to create database, tables and import all the records.</li>
        </ol>
    </li>

</ul>