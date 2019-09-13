
# coding: utf-8

# In[1]:


df = spark.read.csv("FileStore/tables/Project/500_Cities-12468.csv",
                   header=True, sep=',', inferSchema=True)


# In[2]:


df.columns


# In[3]:


#couldnot show the whole data due to memory issues
df.show(df.count(), False)


# In[4]:


df.count()


# In[5]:


len(df.columns)


# In[6]:


df.show(5)


# In[7]:


df.describe("Category").show()



# In[8]:


df.describe("Measure").show()


# In[9]:


df.describe("CategoryID").show()


# In[10]:


df.describe("PopulationCount").show()


# In[11]:


df.select("MeasureID", "CategoryID").distinct().show(df.count(), False)


# In[12]:


df2 = df.select("Year", "StateDesc", "Category", "Measure", "Data_Value_Type", "MeasureId")


# In[13]:


df2.show(5)


# In[14]:


#More records on health outcomes on 2016 than 2015
df2.filter(df.Category == 'Health Outcomes').groupBy("Year").count().show()


# In[15]:


#Filtering rows that we need for analysis

DF4 = df2.filter( (df2["Data_Value_Type"] == "Crude prevalence") & (df2["StateDesc"] != "United States"))
DF4.show(DF4.count(), False)


# In[16]:


DF4.filter(df.Category.isNull()).count()
#DF4.show()


# In[17]:


DF4.filter(df.Measure.isNull()).count()


# In[18]:


#Total number of chronic illness(Health outcomes) recorded in each state in 2015
D = DF4.filter( (DF4["Year"] == "2015") & (DF4["Category"] == "Health Outcomes")).groupBy("StateDesc").count()
D.show(D.count(), False)


# In[19]:


# All cases of chronic diseases recorded in each state 2015 and 2016
DF10 = DF4.filter(DF4.Category == 'Health Outcomes').groupBy("StateDesc", "Year").count()
DF10.show(DF10.count(), False)


# In[20]:


DF12.count()
DF10.count()


# In[21]:


#join not possible due to unequal number of rows in dataframes
DF10.join(DF12, DF10.Year == DF12.Year).select(DF10["*"],DF12["count"]).show()

#df1.join(df2, df1.id == df2.id).select(df1["*"],df2["other"])


# In[22]:


# all health outcomes 2015 and 2016
DF6 = DF4.filter(DF4.Category == 'Health Outcomes').groupBy("StateDesc").count()
DF6.show(DF6.count(), False)


# In[23]:


dft2 = DF12.filter(DF12['Year'] == '2016')
dft2.show()


# In[24]:


DF12 = DF4.filter(DF4.Category == 'Unhealthy Behaviors').groupBy("StateDesc", "Year").count()
DF12.show(DF12.count(), False)


# In[25]:


DF10.count()


# In[26]:


df11 = DF12.withColumnRenamed('count', 'unhealthycount')
df11.show()


# In[27]:


DF11 = DF4.filter(DF4.Category == 'Prevention').groupBy("StateDesc", "Year").count()
DF11.show(DF11.count(), False)


# In[28]:


df1 = df2.groupBy("MeasureId").count()
df1.show(df1.count(), False)


# In[29]:


#df2.filter(df2.category == 'Unhealthy Behaviors').show()
df2.filter(df2.Category == 'Prevention').groupBy("Year").count().show()


# In[30]:


d = df2.groupBy("MeasureId").count()
d.show(d.count(), False)


# In[31]:


#save file as csv.
#df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/filestore/Project/500_cities.csv")


# In[32]:


#second dataset Death rate causes in USA
d = spark.read.csv("FileStore/tables/Project/NCHS___Leading_Causes_of_Death__United_States-87e5e.csv",
                   header=True, sep=',', inferSchema=True)


# In[33]:


d.show(d.count(), False)


# In[34]:


d1 = d.select("Year", "Cause Name", "State", "Deaths")
d1.show()


# In[35]:


d2 = d1.filter( (d1["Year"] == "2015") | (d1["Year"] == "2016"))
d2.show()


# In[36]:


d3 = d2.filter((d2["Cause Name"] != "All causes") & (d2["State"] != "United States")) 
d3.show(d3.count(), False)


# In[37]:


d3.createOrReplaceTempView("NoOfDeathsUS")


# In[38]:


dsql=spark.sql("SELECT State, Year, sum(Deaths) as TotalnumberOfDeaths from NoOfDeathsUS GROUP BY State, Year") 
dsql.show(dsql.count(), False)


# In[39]:


#Sum total number of deaths in USA 
d4=d3.filter(d3.Year == "2016").groupBy("State").sum("Deaths")
d4.show(d4.count(), False) 


# In[40]:


d3.show()


# In[41]:


d5=d3.groupBy("Year").sum("Deaths") 
d5.show() 


# In[42]:


d6 = d3.select("Year", "Cause Name", "State", "Deaths")
d6.show(d6.count(), False)


# In[43]:


#save file as csv.
DF12.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/FileStore/Project/UnhealthyBehaviours.csv")

