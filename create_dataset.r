##########################################################################################
## HarvardX Data Science Capstone CYOP Project Skillability.
## 
## Name: create_dataset.r
## Description: This file listing provides fully automated Stackoverflow data download, 
##              extraction, parsing, cleaning and transformation delivering the final dataset 
##              as a few RDS files.
## Recommendation: Run on Linux tested on Ubuntu 18.04
## Requirements: The following system dependencies are required and must be installed 
##               before running: `wc`, `split`, `awk`, `7z`, `rename`, `mv`, `grep` 
##               and `time`.
## Author: Giovanni Azua Garcia <giovanni.azua@outlook.com>
## Code License: AGPL v3.0 https://www.gnu.org/licenses/agpl-3.0.en.html
## Data License: CC BY-SA 4.0 https://creativecommons.org/licenses/by-sa/4.0/
##
##########################################################################################

# clean the environment
rm(list = ls())
# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## Install and load required library dependencies
##########################################################################################

if(!require(tidyverse)) install.packages("tidyverse", repos = "http://cran.us.r-project.org")
if(!require(data.table)) install.packages("data.table", repos = "http://cran.us.r-project.org")
if(!require(tictoc)) install.packages("tictoc", repos = "http://cran.us.r-project.org")
if(!require(lubridate)) install.packages("lubridate", repos = "http://cran.us.r-project.org")
if(!require(stringr)) install.packages("stringr", repos = "http://cran.us.r-project.org")
if(!require(doMC)) install.packages("doMC", repos = "http://cran.us.r-project.org")
if(!require(parallel)) install.packages("parallel", repos = "http://cran.us.r-project.org")
if(!require(xml2)) install.packages("xml2", repos = "http://cran.us.r-project.org")
if(!require(readr)) install.packages("readr", repos = "http://cran.us.r-project.org")
if(!require(rstudioapi)) install.packages("rstudioapi", repos = "http://cran.us.r-project.org")
if(!require(ggmap)) install.packages("ggmap", repos = "http://cran.us.r-project.org")
if(!require(here)) install.packages("here", repos = "http://cran.us.r-project.org")

##########################################################################################
## Setup initial global values
##########################################################################################

# register cores for parallel processing
ncores <- detectCores()
registerDoMC(ncores)

# set working path to this file's path
if (rstudioapi::isAvailable()) {
  currentPath <- rstudioapi::getActiveDocumentContext()$path
} else {
  currentPath <- here()
}
setwd(dirname(currentPath))
print(getwd())

xmlDir <- file.path("data", "xml")

##########################################################################################
## Define important reusable functions e.g. the portable.set.seed(...)
##########################################################################################

# Portable set.seed function (across R versions) implementation
# @param seed the seed number
portable.set.seed <- function(seed) {
  if (R.version$minor < "6") {
    set.seed(seed)
  } else {
    set.seed(seed, sample.kind="Rounding")
  }
}

# Returns a tibble from the extracted XML file data using xml2. This is a fast low 
# memory implementation that reads huge XML files in chunks of lines. It runs really 
# fast while keeping the memory footprint low. The trick is to convert every
# chunk of XML lines into a valid XML by wrapping it into a <xml> enclosure 
# that can be parsed using xml2.
#
# @param file full file path e.g. "home/username/Downloads/xml/users.xml"
# @param label the label to use for output messages e.g. "Users"
# @param chunkSize the chunks size in number of lines to load per core e.g. 1000
# @param mapping a mapping from input XML attribute names to end tibble column names.
# @returns a tibble object containing the data specified in mapping.
#
extractDataFromXml2 <- function(file, label=NULL, chunkSize=1000, mapping) {
  stopifnot(chunkSize >= 3)
  if (is.null(label)) {
    label <- str_remove(basename(file), "\\.xml")
  }
  tic(sprintf('loading and parsing "%s" data', label))
  result <- read_lines_chunked(file=file, chunk_size=chunkSize, callback = DataFrameCallback$new(function(xmlvec, pos) {
    xml <- xml_children(read_xml(paste("<xml>", paste(xmlvec, collapse=""), "</xml>")))
    y <- xml_attrs(xml[[1]])[mapping]
    if (any(is.na(names(y)))) {
      y <- y[-which(is.na(names(y)))]
    }
    y[setdiff(mapping, names(y))] <- NA
    y <- y[order(factor(names(y), levels=mapping))]
    partial <- as.data.frame(do.call(
      rbind,
      c(list(y),
        lapply(xml[2:length(xml)],
               function(x) {
                 xml_attrs(x)[mapping]
               }
        )
      )
    ), stringsAsFactors = FALSE)
    partial %>%
      select(!!! mapping)
  }))
  toc()
  return(result)
}

# Returns rating data for each conversion use-case e.g. badge "Gold Great Answer" to 5.0.
# Each use-case is computed and loaded into `t`. The purpose of this function is to parallelise 
# the expensive expansion of rows using `separate_rows` i.e. the tags column are converted 
# from `"java|maven|jee5|ci"` to four rows containing the atomic value java, maven, jee5, ci 
# respectively.
#
# @param labelthe label to use to identify the use-case for logging/debugging purposes.
# @param t the input use-case e.g. the badge "Gold Great Answer" to 5.0
# @param rating the rating numeric value assigned to the use-case e.g. 5.0
# @param mainSkills only skills within this set are considered by using semi_join e.g. tibble(skill=c("java"))
# @param ncores the number of cores. 
# @param antit the existing accumulated ratings set to do anti_join against and thus, 
#        eliminate duplicates early in the proccess.
# @param blockSize the block size in rows each core will handle e.g. 8000
# @returns The `separate_rows`-ed data.
# 
parBlockSeparate <- function(label, t, rating, mainSkills, ncores=detectCores(), antit=NULL, blockSize=8000) {
  stopifnot("userId" %in% colnames(t))
  stopifnot("tags" %in% colnames(t))
  stopifnot("creationDate" %in% colnames(t))
  
  if (is.null(antit)) {
    antit <- tibble(userId=as.integer(c()), skill=as.character(c()))
  }
  
  stopifnot("userId" %in% colnames(antit))
  stopifnot("skill" %in% colnames(antit))
  
  # expand the dataset with the post type and id
  if ('answerId' %in% colnames(t)) {
    # it's an answer
    t <- t %>%
      mutate(postId=answerId)
    postType <- 'answer'
    
  } else {
    # it's a question
    t <- t %>%
      mutate(postId=questionId)
    postType <- 'question'
  }
  
  # add row number for blocking to work
  t <- t %>%
    select(userId, tags, creationDate, postId) %>%
    mutate(row = row_number())
  
  nblocks <- ceiling(nrow(t) / blockSize)
  tic(sprintf(paste(label, ', parallel processing %d blocks of size %d', sep=""), 
              rating, nblocks, blockSize))
  resultList <- mclapply(0:nblocks, mc.cores = ncores, FUN = function(i) {
    t %>%
      filter(i*blockSize <= row & row < (i + 1)*blockSize) %>%
      separate_rows(tags, sep="\\|") %>%
      rename(skill=tags) %>%
      mutate(skill=as.character(skill)) %>%
      semi_join(mainSkills, by="skill") %>%
      anti_join(antit, by=c("userId", "skill")) %>%
      group_by(userId, skill) %>%
      summarise(creationDate=min(creationDate), postId=min(postId)) %>%
      ungroup()
  })
  result <- do.call(rbind, resultList)
  result <- result %>%
    group_by(userId, skill) %>%
    summarise(creationDate=min(creationDate), postId=min(postId)) %>%
    ungroup() %>%
    mutate(rating=rating)
  result$postType <- postType
  rm(resultList)
  # trigger garbage collection and free some memory if possible
  gc(TRUE, TRUE, TRUE)
  toc()
  cat(sprintf('DEBUG: %d total number of rows\n', nrow(result)))
  cat(sprintf('DEBUG: %d unique users\n', length(unique(result$userId))))
  return(result)
}

# Downloads the file from the given url, then unzip it using 7z and extract the xml row lines using
# command line tools.
# @param url the url to download.
# @param downloadDir the target directory to download the files to e.g. "7z"
# @param outputDir the output directory to extract the xml files to e.g. "xml"
# @param prefixDir the prefix dir to enclose all the needed directories.
# @param grepForRows whether to grep for "<row " or not e.g. TRUE
# 
downloadExtractAndProcessXml <- function(url, downloadDir="7z", outputDir="xml", 
                                         prefixDir="data", grepForRows=TRUE) {
  fileName <- basename(url)
  downloadDir <- file.path(prefixDir, downloadDir)
  outputDir <- file.path(prefixDir, outputDir)
  if (!dir.exists(downloadDir)) {
    cat(sprintf("creating missing download directory \"%s\" ...\n", downloadDir))
    dir.create(downloadDir, recursive = T)
  }
  if (!dir.exists(outputDir)) {
    cat(sprintf("creating missing output directory \"%s\" ...\n", outputDir))
    dir.create(outputDir, recursive = T)
  }
  dl <- file.path(downloadDir, fileName)
  # download only if necessary
  if (!file.exists(dl)) {
    download.file(url, dl)
  } else {
    cat(sprintf("\"%s\" exists already, skipping download ...\n", basename(dl)))
  }
  # extract and process only if necessary
  xmlFileName <- paste(str_remove(str_remove(fileName, "\\.7z"), ".*-"), ".xml", sep = "")
  fullFilePath <- file.path(getwd(), outputDir, xmlFileName)
  if (!file.exists(fullFilePath)) {
    system(sprintf("7z e -o\"%s\" %s > /dev/null", file.path(getwd(), outputDir), dl))
    if (grepForRows) {
      system(sprintf("grep \"<row\" \"%s\" > tmp && mv tmp \"%s\"", fullFilePath, 
                     fullFilePath))
    }
  } else {
    cat(sprintf("\"%s\" exists already, skipping unzip and further processing ...\n", 
                basename(fullFilePath)))
  }
}

# Returns the file path for the given object name.
#
# @param objectName the name of the object e.g. "Users"
# @param prefixDir the prefix directory where all data is stored e.g. "data"
# @param rdsDir the directory where the RDS files are located e.g. "rds"
# @param ext the extension for the RDS files i.e. ".rds"
# @returns the file path for the given dataset name.
filePathForObjectName <- function(objectName, prefixDir="data", 
                                  rdsDir="rds", ext=".rds") {
  rdsFolder <- file.path(prefixDir, rdsDir)
  if (!dir.exists(rdsFolder)) {
    dir.create(rdsFolder, recursive = T)
  }
  fileName <- paste(objectName, ext, sep="")
  filePath <- file.path(rdsFolder, fileName)
  return(filePath)  
}

# Returns the object (dataset or otherwise) by name, it will either load the dataset from an 
# RDS file if it exists or download it from GitHub automatically. If downloaded then the file 
# will be created in the expected location so that we won't be downloading it again.
#
# @param objectName the name of the dataset e.g. "Users"
# @param prefixDir the prefix directory where all data is stored e.g. "data"
# @param rdsDir the directory where the RDS files are located e.g. "rds"
# @param ext the extension for the RDS files i.e. ".rds"
# @param baseUrl the base GitHub url where the data is located.
# @param userName the GitHub user name e.g. "bravegag"
# @param repoName the GitHub repository name e.g. "HarvardX-Skillability"
# @param branchName the GitHub branch name e.g. "master"
# @returns the dataset by name.
readObjectByName <- function(objectName, prefixDir="data", rdsDir="rds", ext=".rds", 
                             userName="bravegag", repoName="HarvardX-Skillability", branchName="master", 
                             baseUrl="https://github.com/%s/%s/blob/%s/data/rds/%s?raw=true") {
  filePath <- filePathForObjectName(objectName = objectName, prefixDir = prefixDir, 
                                    rdsDir = rdsDir, ext = ext)
  fileName <- basename(filePath)
  if (!file.exists(filePath)) {
    # download the file
    url <- sprintf(baseUrl, userName, repoName, branchName, fileName)
    cat(sprintf("downloading \"%s\"\n", url))
    download.file(url, filePath, extra="L")
  } else {
    cat(sprintf("object \"%s\" exists, skipping download ...\n", filePath))
  }
  return(readRDS(filePath))
}

# Saves the object (dataset or otherwise) by name, the required folders will be 
# created if they don't already exist.
#
# @param object the object e.g. tibble or data frame
# @param objectName the name of the object e.g. "Users"
# @param prefixDir the prefix directory where all data is stored e.g. "data"
# @param rdsDir the directory where the RDS files are located e.g. "rds"
# @param ext the extension for the RDS files i.e. ".rds"
saveObjectByName <- function(object, objectName, prefixDir="data", 
                             rdsDir="rds", ext=".rds") {
  filePath <- filePathForObjectName(objectName = objectName, prefixDir = prefixDir, 
                                    rdsDir = rdsDir, ext = ext)
  saveRDS(object=object, file=filePath)
}

##########################################################################################
## Download, parse and transform the Tags data
##########################################################################################

downloadExtractAndProcessXml(url="https://archive.org/download/stackexchange/stackoverflow.com-Tags.7z")
mapping <- c("Id", "TagName", "Count")
names(mapping) <- c("tagId", "tag", "count")
tags <- extractDataFromXml2(file=file.path("data", "xml", "Tags.xml"),
                            chunkSize = 1000, 
                            mapping = mapping)
tags <- as_tibble(tags) %>%
  mutate(tagId=as.integer(tagId),
         count=as.numeric(count))
str(tags)

# expected number of tags
stopifnot(nrow(tags) == 56525)

saveObjectByName(tags, "Tags")

##########################################################################################
## Download, parse and transform the Users data
##########################################################################################

downloadExtractAndProcessXml(url="https://archive.org/download/stackexchange/stackoverflow.com-Users.7z")
out <- system(command=sprintf("wc -l %s/Users.xml | awk '{print $1;}'", xmlDir), intern = TRUE)
splitSize <- ceiling(as.numeric(out) / ncores)
system(command=sprintf("split -l%s -d %s/Users.xml %s/Users_ --verbose && rename s/$/.xml/ %s/Users_*", 
                       as.character(splitSize), xmlDir, xmlDir, xmlDir))

mapping <- c("Id", "Reputation", "CreationDate", "LastAccessDate", "Location", "Views", 
             "UpVotes", "DownVotes")
names(mapping) <- c("userId", "reputation", "creationDate", "lastAccessDate", "location", "views", 
                    "upvotes", "downvotes")

tic('loading and parsing Users.xml in parallel')
resultList <- foreach(i=0:(ncores - 1)) %dopar% {
  partial <- extractDataFromXml2(file=sprintf("%s/Users_0%d.xml", xmlDir, i),
                                 chunkSize = 1000, 
                                 mapping = mapping)
  partial <- partial %>%
    mutate(userId=as.integer(userId), 
           reputation=as.numeric(reputation),
           creationDate=as_datetime(creationDate),
           lastAccessDate=as_datetime(lastAccessDate),
           views=as.integer(views), 
           upvotes=as.integer(upvotes), 
           downvotes=as.integer(downvotes)) %>%
    filter(reputation > 999 |                 # trims the dataset to a total of ~200k instead of ~11.37m users 
             (str_detect(tolower(location), 
                         "(\\bch\\b|switzerland|schweiz|svizzera|suisse|svizra)") & reputation > 100))
  return(partial)
}
users <- do.call(bind_rows, resultList)
rm(resultList)
toc()
str(users)

# fix myself to have a location
users <- users %>%
  filter(userId == 1142881) %>%
  mutate(location="Leimbach, Switzerland") %>%
  bind_rows(users %>% filter(userId != 1142881))

# expected number of users
stopifnot(nrow(users) == 200630)

# expected reputation mean
stopifnot(abs(mean(users$reputation) - 5498.885) < 1e-5)

saveObjectByName(users, "Users")
system(command=sprintf("rm %s/Users_0*.xml", xmlDir))

# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## create a smaller subset of Users containing only the users in Switzerland
##########################################################################################

# do this only if the file isn't there to avoid costly Google geomapping calls
if (!file.exists(filePathForObjectName("UsersCH"))) {
  # the environment variable GOOGLE_API_KEY is required or simply copy-paste your
  # google key instead. To obtain a google key, follow the steps outlined here:
  # https://developers.google.com/maps/documentation/javascript/get-api-key
  register_google(key=Sys.getenv("GOOGLE_API_KEY"))
  
  # get users whose location is Switzerland only
  usersCh <- users %>%
    filter(str_detect(tolower(location), "(\\bch\\b|switzerland|schweiz|svizzera|suisse|svizra)")) %>%
    arrange(desc(reputation))
  
  # get the unique locations so that we avoid duplicate calls e.g. "Zurich, Switzerland"
  swissLocations <- usersCh %>%
    select(location) %>%
    unique()
  # WARNING! this code paired with a valid GOOGLE_API_KEY may cost money!
  swissLocations <- mutate_geocode(swissLocations, location = location)
  usersCh <- usersCh %>%
    left_join(swissLocations, by="location")
  
  # write the usersCh to disk
  saveObjectByName(usersCh, "UsersCH")
}
usersCh <- readObjectByName("UsersCH")
# expected number of Swiss users
stopifnot(nrow(usersCh) == 4258)

##########################################################################################
## Download, parse and transform the Badges data
##########################################################################################

# read dependencies
tags <- readObjectByName("Tags")
users <- readObjectByName("Users")

downloadExtractAndProcessXml(url="https://archive.org/download/stackexchange/stackoverflow.com-Badges.7z")
out <- system(command=sprintf("wc -l %s/Badges.xml | awk '{print $1;}'", xmlDir), intern = TRUE)
splitSize <- ceiling(as.numeric(out) / ncores)
system(command=sprintf("split -l%s -d %s/Badges.xml %s/Badges_ --verbose && rename s/$/.xml/ %s/Badges_*", 
                       as.character(splitSize), xmlDir, xmlDir, xmlDir))

mapping <- c("UserId", "Name", "Date", "Class")
names(mapping) <- c("userId", "badge", "date", "class")

tic('loading and parsing Badges.xml in parallel')
resultList <- foreach(i=0:(ncores - 1)) %dopar% {
  partial <- extractDataFromXml2(file=sprintf("%s/Badges_0%d.xml", xmlDir, i),
                                 chunkSize = 1000, 
                                 mapping = mapping)
  partial <- as_tibble(partial) %>%
    mutate(userId=as.integer(userId),
           date=as_datetime(date),
           class=as.factor(recode(class, `1`="gold", `2`="silver",`3`="bronze"))) %>%
    mutate(tag=badge) %>%
    anti_join(tags, by="tag") %>%
    semi_join(users, by="userId") %>%
    select(userId, badge, date, class)
  return(partial)
}
badges <- do.call(bind_rows, resultList)
badges <- badges %>% 
  mutate(badge=as.factor(badge))
rm(resultList)
toc()
str(badges)

# expected number of badges
stopifnot(nrow(badges) == 12597503)

saveObjectByName(badges, "Badges")
system(command=sprintf("rm %s/Badges_0*.xml", xmlDir))

# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## Download and extract the Posts.xml data
##########################################################################################

downloadExtractAndProcessXml(url="https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z", 
                             grepForRows=FALSE)

##########################################################################################
## Load, parse and transform and trim the Answers data
##########################################################################################

# read dependencies
users <- readObjectByName("Users")

if (!file.exists(sprintf("%s/Answers.xml", xmlDir))) {
  # only Answer posts have "ParentId" so we grep for it (though the best but slower would be PostTypeId=\"2\")
  system(command=sprintf("time grep \"ParentId\" %s/Posts.xml > %s/Answers.xml", xmlDir, xmlDir))
}
out <- system(command=sprintf("wc -l %s/Answers.xml | awk '{print $1;}'", xmlDir), intern = TRUE)
splitSize <- ceiling(as.numeric(out) / ncores)
system(command=sprintf("split -l%s -d %s/Answers.xml %s/Answers_ --verbose && rename s/$/.xml/ %s/Answers_*", 
                       as.character(splitSize), xmlDir, xmlDir, xmlDir))

mapping <- c("Id", "ParentId", "CreationDate", "Score", 
             "OwnerUserId", "LastActivityDate", "CommentCount")
names(mapping) <- c("answerId", "questionId", "creationDate", "score", 
                    "userId", "lastActivityDate", "commentCount")

tic('loading and parsing Answers.xml in parallel')
resultList <- foreach(i=0:(ncores - 1)) %dopar% {
  partial <- extractDataFromXml2(file=sprintf("%s/Answers_0%d.xml", xmlDir, i),
                                 chunkSize = 1000, 
                                 mapping = mapping)
  partial <- partial %>%
    mutate(answerId=as.integer(answerId),
           questionId=as.integer(questionId),
           creationDate=as_datetime(creationDate),
           score=as.numeric(score),
           userId=as.integer(userId),
           lastActivityDate=as_datetime(lastActivityDate),
           commentCount=as.integer(commentCount)) %>%
    semi_join(users, by="userId") %>%
    filter(score > 2 & !is.na(questionId)) # trims the dataset to a total of 4.83m instead of ~28.25m answers 
  return(partial)
}
answers <- do.call(bind_rows, resultList)
rm(resultList)
toc()
str(answers)

# expected number of answers
stopifnot(nrow(answers) == 4830031)

saveObjectByName(answers, "Answers")
system(command=sprintf("rm %s/Answers_0*.xml", xmlDir))

# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## Load, parse and transform and trim the Questions data
##########################################################################################

# read dependencies
users <- readObjectByName("Users")
answers <- readObjectByName("Answers")

if (!file.exists(sprintf("%s/Questions.xml", xmlDir))) {
  # only Question posts have "AnswerCount" so we grep for it (though the best but slower would be PostTypeId=\"1\")
  system(command=sprintf("time grep \"AnswerCount\" %s/Posts.xml > %s/Questions.xml", xmlDir, xmlDir))
}
out <- system(command=sprintf("wc -l %s/Questions.xml | awk '{print $1;}'", xmlDir), intern = TRUE)
splitSize <- ceiling(as.numeric(out) / ncores)
system(command=sprintf("split -l%s -d %s/Questions.xml %s/Questions_ --verbose && rename s/$/.xml/ %s/Questions_*", 
                       as.character(splitSize), xmlDir, xmlDir, xmlDir))

mapping <- c("Id", "AcceptedAnswerId", "CreationDate", "Score", 
             "ViewCount", "OwnerUserId", "LastActivityDate", "Tags", "AnswerCount", 
             "CommentCount", "FavoriteCount")
names(mapping) <- c("questionId", "acceptedAnswerId", "creationDate", "score", 
                    "viewCount", "userId", "lastActivityDate", "tags", "answerCount", 
                    "commentCount", "favoriteCount")
tic('loading and parsing Questions.xml in parallel')
resultList <- foreach(i=0:(ncores - 1)) %dopar% {
  partial <- extractDataFromXml2(file=sprintf("%s/Questions_0%d.xml", xmlDir, i),
                                 chunkSize = 1000, 
                                 mapping = mapping)
  partial <- partial %>%
    mutate(questionId=as.integer(questionId),
           acceptedAnswerId=as.integer(acceptedAnswerId),
           creationDate=as_datetime(creationDate),
           score=as.numeric(score),
           viewCount=as.integer(viewCount),
           userId=as.integer(userId),
           lastActivityDate=as_datetime(lastActivityDate),
           tags=str_replace_all(str_remove_all(tags, "<"), ">", "|"),
           tags=str_sub(tags, 1, str_length(tags)-1),
           answerCount=as.integer(answerCount),
           commentCount=as.integer(commentCount),
           favoriteCount=as.integer(favoriteCount)) %>%
    filter(answerCount > 0)
  
  # questions answered by the "users" selection
  partial1 <- partial %>%
    semi_join(answers %>% select(questionId) %>% unique(), by="questionId")
  
  # questions asked by the "users" selection
  partial2 <- partial %>%
    semi_join(users %>% select(userId) %>% unique(), by="userId") %>%
    anti_join(partial1 %>% select(questionId) %>% unique(), by="questionId") %>%
    filter(score > 0) # trims the dataset to a total of 5.39m instead of ~18.59m questions
  
  return(bind_rows(partial1, partial2))
}
questions <- do.call(bind_rows, resultList)
rm(resultList)
toc()
str(questions)

# expected number of questions
stopifnot(nrow(questions) == 5393941)

saveObjectByName(questions, "Questions")
system(command=sprintf("rm %s/Questions_0*.xml", xmlDir))

# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## Generate the user-skill ratings dataset
##########################################################################################

# read dependencies
tags <- readObjectByName("Tags")
badges <- readObjectByName("Badges")
answers <- readObjectByName("Answers")
questions <- readObjectByName("Questions")

# select the top 2k skills by count
mainSkills <- tags %>% 
  top_n(2000, count) %>%
  rename(skill=tag) %>% 
  arrange(desc(count))

# the top 2k skills contain 82% of the counts
100*sum(mainSkills$count)/sum(tags$count)

# users rated as 5.0 in those skills:
# - have achieved gold Populist badge on answers requiring those skills
acceptedAnswers <- questions %>% 
  select(questionId, acceptedAnswerId) %>%
  inner_join(answers %>% select(answerId, questionId, score), by="questionId") %>%
  filter(acceptedAnswerId == answerId) %>%
  select(answerId, questionId, score)
ratings <- parBlockSeparate('gold Populist Answer %.1f', badges %>% 
                              filter(class == "gold" & badge == "Populist") %>%
                              inner_join(answers %>% filter(score >= 10), by="userId") %>%
                              anti_join(acceptedAnswers, by="answerId") %>%
                              rename(scorePopulist=score) %>%
                              inner_join(acceptedAnswers %>% select(questionId, score), by="questionId") %>%
                              filter(scorePopulist >= 2*score) %>%
                              inner_join(questions %>% select(questionId, tags), by="questionId"), 
                            rating = 5.0, mainSkills = mainSkills, ncores = ncores)
# ensure we detect correctly only answer post types
stopifnot(ratings %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(ratings %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
rm(acceptedAnswers)
nrow(ratings)
mean(ratings$rating)

# users rated as 5.0 in those skills:
# - have achieved gold Great Answer badge on questions requiring those skills
results <- parBlockSeparate('gold Great Answer to %.1f', badges %>% 
                              filter(class == "gold" & badge == "Great Answer") %>% 
                              inner_join(answers %>% filter(score >= 100), by="userId") %>%
                              inner_join(questions %>% select(questionId, tags), by="questionId"), 
                            rating = 5.0, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only answer post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 4.5 in those skills:
# - have achieved a gold Great Question badge on questions requiring those skills
results <- parBlockSeparate('gold Great Question %.1f', badges %>% 
                              filter(class == "gold" & badge == "Great Question") %>%
                              inner_join(questions %>% filter(score >= 100), by="userId"), 
                            rating = 4.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 4.5 in those skills:
# - have achieved silver Guru badge on answers requiring those skills
results <- parBlockSeparate('silver Guru Answer %.1f', badges %>% 
                              filter(class == "silver" & badge == "Guru") %>%
                              inner_join(answers %>% filter(40 <= score & score < 100), by="userId") %>%
                              inner_join(questions %>% select(questionId, acceptedAnswerId, tags), by="questionId") %>%
                              filter(answerId == acceptedAnswerId), 
                            rating = 4.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only answer post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 4.5 in those skills:
# - have achieved silver Good Answer badge on questions requiring those skills
results <- parBlockSeparate('silver Good Answer %.1f', badges %>% 
                              filter(class == "silver" & badge == "Good Answer") %>%
                              inner_join(answers %>% filter(25 <= score & score < 100), by="userId") %>%
                              inner_join(questions %>% select(questionId, tags), by="questionId"), 
                            rating = 4.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only answer post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

### NOTE: The following needs to be split into three separate sets because despite the 
### blocking >128m it won't fit in memory (use the userId as hash code to segregate)

userBlocks <- 3
for (userBlock in 0:(userBlocks - 1)) {
  # users rated as 4.0 in those skills:
  # - have achieved bronze Nice Answer badge on questions requiring those skills
  cat(sprintf("processing user block %d\n", userBlock))
  results <- parBlockSeparate('bronze Nice Answer %.1f', badges %>% 
                                filter(class == "bronze" & badge == "Nice Answer" & userId %% userBlocks == userBlock) %>%
                                inner_join(answers %>% filter(10 <= score & score < 25), by="userId") %>%
                                inner_join(questions %>% select(questionId, tags), by="questionId"), 
                              rating = 4.0, mainSkills = mainSkills, ncores = ncores, antit = ratings)
  # ensure we detect correctly only answer post types
  stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
  stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
  ratings <- bind_rows(ratings, results)
}
nrow(ratings)
mean(ratings$rating)

# users rated as 3.5 in those skills
# - have answers with score between [5, 10)
results <- parBlockSeparate('answers with score between [5, 10) %.1f', answers %>%
                              filter(5 <= score & score < 10) %>%
                              inner_join(questions %>% select(questionId, tags), by="questionId"), 
                            rating = 3.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only answer post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 3.0 in those skills
# - have answers with score between [0, 5)
results <- parBlockSeparate('answers with score between [0, 5) %.1f', answers %>%
                              filter(0 <= score & score < 5) %>%
                              inner_join(questions %>% select(questionId, tags), by="questionId"), 
                            rating = 3.0, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only answer post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'answer')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 2.5 in those skills
# - have achieved a silver Good Question badge on questions requiring those skills
results <- parBlockSeparate('silver Good Question %.1f', badges %>% 
                              filter(class == "silver" & badge == "Good Question") %>%
                              inner_join(questions %>% filter(25 <= score & score < 100), by="userId"), 
                            rating = 2.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 2.5 in those skills
# - have achieved a bronze Nice Question badge on questions requiring those skills
results <- parBlockSeparate('bronze Nice Question %.1f', badges %>% 
                              filter(class == "bronze" & badge == "Nice Question") %>%
                              inner_join(questions %>% filter(10 <= score & score < 25), by="userId"), 
                            rating = 2.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 2.5 in those skills
# - have questions with score between [5, 10)
results <- parBlockSeparate('questions with score between [5, 10) %.1f', questions %>%
                              filter(5 <= score & score < 10 & viewCount >= 2500), 
                            rating = 2.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 2.0 in those skills
# - have questions with score between [2, 5)
results <- parBlockSeparate('questions with score between [2, 5) %.1f', questions %>%
                              filter(2 <= score & score < 5 & viewCount >= 2500),
                            rating = 2.0, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

# users rated as 1.5 in those skills
# - have questions with score between [0, 2)
results <- parBlockSeparate('questions with score between [0, 2) %.1f', questions %>%
                              filter(0 <= score & score < 2 & viewCount >= 2500),
                            rating = 1.5, mainSkills = mainSkills, ncores = ncores, antit = ratings)
# ensure we detect correctly only question post types
stopifnot(results %>% select(postType) %>% unique() %>% count() == 1)
stopifnot(results %>% select(postType) %>% unique() %>% pull(postType) == 'question')
ratings <- bind_rows(ratings, results)
nrow(ratings)
mean(ratings$rating)

users <- readObjectByName("Users")

# clean up the final ratings dataset:
# * keep only users within the users set and not the ones with questions-only
# * transform postType to a factor
ratings <- ratings %>%
  semi_join(users, by="userId") %>%
  mutate(postType=as.factor(postType)) %>%
  select(userId, skill, creationDate, postId, postType, rating)
nrow(ratings)
mean(ratings$rating)

# consistency check: only one rating per distinct user and skill
stopifnot(nrow(ratings %>% group_by(userId, skill) %>% filter(n() > 1)) == 0)

# takes ~16m
tic("generating the first post date rating feature")
questionPosts <- questions %>%
  rename(postId=questionId) %>%
  select(postId, userId, tags, creationDate) %>%
  semi_join(ratings %>% select(userId) %>% distinct(), by="userId") %>%
  mutate(postType="question") %>%
  separate_rows(tags, sep="\\|") %>%
  rename(skill=tags) %>%
  mutate(skill=as.character(skill)) %>%
  semi_join(mainSkills, by="skill")

answerPosts <- answers %>%
  rename(postId=answerId) %>%
  inner_join(questions %>% select(questionId, tags), by="questionId") %>%
  select(postId, userId, tags, creationDate) %>%
  semi_join(ratings %>% select(userId) %>% distinct(), by="userId") %>%
  mutate(postType="answer") %>%
  separate_rows(tags, sep="\\|") %>%
  rename(skill=tags) %>%
  mutate(skill=as.character(skill)) %>%
  semi_join(mainSkills, by="skill")

allPosts <- questionPosts %>%
  bind_rows(answerPosts)

firstPostsDate <- allPosts %>% 
  group_by(userId, skill) %>%
  summarise(firstPostDate=min(creationDate)) %>%
  semi_join(ratings, by=c("userId", "skill"))

stopifnot(nrow(ratings) == nrow(firstPostsDate))

ratings <- ratings %>%
  left_join(firstPostsDate, by=c("userId", "skill")) %>%
  select(userId, skill, firstPostDate, creationDate, postId, postType, rating)

rm(questionPosts, answerPosts, allPosts, firstPostsDate)
toc()

# expected total number of ratings
stopifnot(nrow(ratings) == 5442999)
# expected global mean of ratings
stopifnot(abs(mean(ratings$rating) - 3.266489) < 1e-6)
# consistency check: only one rating per distinct user and skill
stopifnot(nrow(ratings %>% group_by(userId, skill) %>% filter(n() > 1)) == 0)

saveObjectByName(ratings, "Ratings")
ratings <- readObjectByName("Ratings")














