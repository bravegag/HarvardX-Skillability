##########################################################################################
## HarvardX Data Science Capstone CYOP Project Skillability.
## 
## Name: skillability.r
## Description: This file listing provides the data exploration, visualisation, modeling,
##              method and result analysis for the Stackoverflow data.
## Recommendation: Unix-like environments to be able to run the parallel multi-core 
##                 implementations needed for caret::train(...) and parallel::mclapply(...)
## Requirements: R 3.6.x installed with all required packages. 
## Author: Giovanni Azua Garcia <giovanni.azua@outlook.com>
## Code License: AGPL v3.0 https://www.gnu.org/licenses/agpl-3.0.en.html
## Data License: CC BY-SA 4.0 https://creativecommons.org/licenses/by-sa/4.0/
## References: https://projecteuclid.org/download/pdf_1/euclid.ss/1032280214
##             http://statweb.stanford.edu/~ckirby/brad/papers/2018Automatic-Construction-BCIs.pdf
##             http://sphweb.bumc.bu.edu/otlt/MPH-Modules/BS/R/R-Manual/R-Manual16.html
##########################################################################################

# clean the environment
rm(list = ls())

# trigger garbage collection and free some memory if possible
gc(TRUE, TRUE, TRUE)

##########################################################################################
## Install and load required library dependencies
##########################################################################################

defaultRepos <- "http://cran.us.r-project.org"

if(!require(tidyverse)) install.packages("tidyverse", repos = defaultRepos)
if(!require(caret)) install.packages("caret", repos = defaultRepos)
if(!require(boot)) install.packages("boot", repos = defaultRepos)
if(!require(purrr)) install.packages("purrr", repos = defaultRepos)
if(!require(data.table)) install.packages("data.table", repos = defaultRepos)
if(!require(tictoc)) install.packages("tictoc", repos = defaultRepos)
if(!require(lubridate)) install.packages("lubridate", repos = defaultRepos)
if(!require(stringr)) install.packages("stringr", repos = defaultRepos)
if(!require(parallel)) install.packages("parallel", repos = defaultRepos)
if(!require(ggplot2)) install.packages("ggplot2", repos = defaultRepos)
if(!require(ggmap)) install.packages("ggmap", repos = defaultRepos)
if(!require(ggrepel)) install.packages("ggrepel", repos = defaultRepos)
if(!require(scales)) install.packages("scales", repos = defaultRepos)
if(!require(RColorBrewer)) install.packages("RColorBrewer", repos = defaultRepos)
if(!require(Metrics)) install.packages("Metrics", repos = defaultRepos)
if(!require(rstudioapi)) install.packages("rstudioapi", repos = defaultRepos)
if(!require(here)) install.packages("here", repos = defaultRepos)

##########################################################################################
## Setup initial global values
##########################################################################################

# check the platform OS type
if(.Platform$OS.type == "unix") {
  # use doMC when it's an Unix-like OS
  if(!require(doMC)) install.packages("doMC", repos = defaultRepos)

  # register cores for parallel processing
  ncores <- detectCores()
  registerDoMC(ncores)
  
} else {
  # NOTE! pending to port multi-core code to Windows
  ncores <- 1
}

# best attempt to set the working path to this file's path
if (rstudioapi::isAvailable()) {
  currentPath <- dirname(rstudioapi::getActiveDocumentContext()$path)
} else {
  # not very accurate, will default to home folder and not to this file's path
  currentPath <- here()
}
setwd(currentPath)
print(getwd())

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
  tryCatch({
    filePath <- filePathForObjectName(objectName = objectName, prefixDir = prefixDir, 
                                      rdsDir = rdsDir, ext = ext)
    fileName <- basename(filePath)
    if (!file.exists(filePath)) {
      # download the file
      url <- sprintf(baseUrl, userName, repoName, branchName, fileName)
      cat(sprintf("downloading \"%s\"\n", url))
      download.file(url, filePath, extra="L", mode="wb")
    } else {
      cat(sprintf("object \"%s\" exists, skipping download ...\n", filePath))
    }
    return(readRDS(filePath))
  }, warning = function(w) {
    cat(sprintf("WARNING - attempting to access or download the %s data:\n%s\n", 
                objectName, w))
    return(NULL)
  }, error = function(e) {
    cat(sprintf("ERROR - attempting to access or download the %s data:\n%s\n", 
                objectName, e))
    return(NULL)
  }, finally = {
    # nothing to do here
  })  
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
## Data exploration and visualization: Basic
##########################################################################################

# load the Users, Questions, Answers, Badges and Tags data files
users <- readObjectByName("Users")
questions <- readObjectByName("Questions")
answers <- readObjectByName("Answers")
badges <- readObjectByName("Badges")
tags <- readObjectByName("Tags")

# what's the question with highest score? 11227809
# https://stackoverflow.com/questions/11227809/
questions %>% 
  top_n(1, score) %>%
  select(questionId, score, answerCount, commentCount, favoriteCount, tags, creationDate)

# what's the answer with highest score? 11227809
answers %>% 
  top_n(1, score) %>%
  select(answerId, score, commentCount, creationDate)

# what's the top user? 22656
# https://stackoverflow.com/users/22656/jon-skeet
users %>%
  top_n(1, reputation) %>%
  select(userId, reputation, creationDate, location, upvotes, downvotes)

# what are the top ten tags / skills?
topTenSkills <- tags %>%
  top_n(10, count) %>%
  arrange(desc(count)) %>%
  rename(skill=tag)
topTenSkills

# what's the average user reputation?
users %>%
  summarise(mean=mean(reputation), median=median(reputation))

# what's the average number of questions per user? 5.35
questions %>%
  group_by(userId) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  summarise(mean=mean(n), median=median(n))

# what's the average number of answers per user? 26.3
answers %>%
  group_by(userId) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  summarise(mean=mean(n), median=median(n))

# what's the average number of answers per question? 2.27
questions %>%
  summarise(mean=mean(answerCount), median=median(answerCount))

# histogram of the log10-transformed of users reputation: positively skewed!
# NOTE! that we excluded the users (located in Switzerland) with reputation less than 999
users %>%
  filter(reputation > 999) %>%
  mutate(reputation=log10(reputation)) %>%
  ggplot(aes(reputation)) + 
  geom_histogram(bins = 200, colour="#377EB8", fill="#377EB8") +
  ggtitle("Users log10 reputation histogram") +
  xlab("log10 reputation") +
  theme(plot.title = element_text(hjust = 0.5),
        legend.text = element_text(size=12),
        axis.text.x = element_text(angle = 45, hjust = 1))

# histogram of the log10-transfored of users reputation per badge and 
# excluding users with less than 999 reputation 
users %>%
  filter(reputation > 999) %>%
  mutate(reputation=log10(reputation)) %>%
  inner_join(badges %>% 
               select(userId, badge) %>% 
               filter(badge %in% c("Populist", "Great Answer", "Guru", "Great Question", 
                                   "Good Answer", "Good Question", "Nice Answer", 
                                   "Nice Question")), by="userId") %>%
  mutate(badge=factor(badge, levels=c("Populist", "Great Answer", "Guru", "Great Question", 
                                      "Good Answer", "Good Question", "Nice Answer", 
                                      "Nice Question"))) %>%
  ggplot(aes(reputation, group=badge, color=badge, fill=badge)) + 
  geom_histogram(bins = 200) +
  ggtitle("Users log10 reputation histogram") +
  xlab("log10 reputation") +
  theme(plot.title = element_text(hjust = 0.5),
        legend.text = element_text(size=12)) +
  facet_wrap(~badge)

##########################################################################################
## Data exploration and visualization: Top skills & technology trends
##########################################################################################

# select the top 2k tags/skills by count
mainSkills <- tags %>% 
  top_n(2000, count) %>%
  rename(skill=tag) %>% 
  arrange(desc(count))

# what's the proportion to the total? 82.3%
100*sum(mainSkills$count)/sum(tags$count)

# smaller subset of questions matching the main tags
questionSkills <- questions %>% 
  filter(score > 9 & viewCount > 99 & answerCount > 1)
# takes ~35s
tic(sprintf('separating rows with %d', nrow(questionSkills)))
questionSkills <- questionSkills %>%
  select(questionId, tags) %>%
  separate_rows(tags, sep="\\|") %>%
  rename(skill=tags) %>%
  inner_join(mainSkills, by="skill") %>%
  arrange(desc(count)) %>%
  select(questionId, skill)
str(questionSkills)
toc()

# takes ~15m if TRUE
if (FALSE) {
  tic(sprintf('computing co-occurrence matrix with %d question-skill', 
              nrow(questionSkills)))
  X <- crossprod(table(questionSkills[1:2]))
  diag(X) <- 0
  toc()
  saveObjectByName(X, "XCo-occurrence")
}
X <- readObjectByName("XCo-occurrence")

# how sparse is it?
sum(X == 0)/(dim(X)[1]^2)

# compute PCA 
pca <- prcomp(X)

# plot the variability of each of the vectors 
pc <- 1:50
qplot(pc, pca$sdev[pc])

# plot the variability explained 
var_explained <- cumsum(pca$sdev^2 / sum(pca$sdev^2))
qplot(pc, var_explained[pc])

# create tibble containing the first four principal components
pcs <- tibble(skill = rownames(pca$rotation), PC1=pca$rotation[,"PC1"], 
              PC2=pca$rotation[,"PC2"])
# highlight the top ten tags
pcs <- pcs %>%
  mutate(fontface=ifelse(skill %in% (topTenSkills %>% pull(skill)), 
                         'bold', 'plain'))

technologies <- c("Blockchain, Cloud, Build & Data Viz",
                  "Full Stack", 
                  "Web Frontend & Mobile",
                  "Microsoft Stack",
                  "Python & C++",
                  "Software Engineering",
                  "Javascript",
                  "iOS Stack",
                  "Other")

# choose 9 colors: 2x4 components plus everything else
colorPalette <- RColorBrewer::brewer.pal(name='Set1', n=9)
colorSpec <- colorPalette[1:9]
names(colorSpec) <- technologies

# maximum tags to choose in each direction
M <- 25
highlight <- pcs %>% 
  arrange(PC1) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[1], pc=1)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(desc(PC1)) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[2], pc=1) %>%
  bind_rows(highlight)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(PC2) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[3], pc=2) %>%
  bind_rows(highlight)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(desc(PC2)) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[4], pc=2) %>%
  bind_rows(highlight)

nonHighlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  mutate(Technology=technologies[9])

# switch to the 3rd and 4rth PCA components
pcs <- tibble(skill = rownames(pca$rotation), PC3=pca$rotation[,"PC3"], 
              PC4=pca$rotation[,"PC4"])
# highlight the top ten tags
pcs <- pcs %>%
  mutate(fontface=ifelse(skill %in% (topTenSkills %>% pull(skill)), 'bold', 'plain'))

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(PC3) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[5], pc=3) %>%
  bind_rows(highlight)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(desc(PC3)) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[6], pc=3) %>%
  bind_rows(highlight)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(PC4) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[7], pc=4) %>%
  bind_rows(highlight)

highlight <- pcs %>% 
  anti_join(highlight, by="skill") %>%
  arrange(desc(PC4)) %>% 
  slice(1:M) %>%
  mutate(Technology=technologies[8], pc=4) %>%
  bind_rows(highlight)

# plot the components in log scale
highlightLog <- highlight %>% 
  mutate(PC1=sign(PC1)*log10(abs(PC1)), 
         PC2=sign(PC2)*log10(abs(PC2)))

nonHighlightLog <- nonHighlight %>% 
  mutate(PC1=sign(PC1)*log10(abs(PC1)), 
         PC2=sign(PC2)*log10(abs(PC2)))

portable.set.seed(1)
highlightLog %>% 
  filter(pc %in% c(1, 2)) %>%
  ggplot(aes(PC1, PC2, label=skill, colour=Technology)) +
  geom_jitter(alpha = 0.4, size = 3) + 
  theme(legend.position="bottom", plot.title = element_text(hjust = 0.5), 
        legend.text=element_text(size=12)) + 
  guides(fill = guide_legend(nrow=2)) +
  xlab(sprintf("sign(PC1) x log10|PC1| - Variance explained %d%%", round(100*pca$sdev[1]^2 / sum(pca$sdev^2)))) + 
  ylab(sprintf("sign(PC2) x log10|PC2| - Variance explained %d%%", round(100*pca$sdev[2]^2 / sum(pca$sdev^2)))) +
  ggtitle("Top Tags (i.e. Skills) in each direction of the first two Principal Components PC1 and PC2") +
  geom_text_repel(aes(fontface=fontface), segment.alpha = 0.3, size = 5,
                  force = 7, nudge_x = 0.1, nudge_y = 0.1, seed = 1, family="Arial") +
  scale_colour_manual(values = colorSpec) +
  scale_x_continuous(limits=c(-8, 8)) +   
  scale_y_continuous(limits=c(-8, 8)) +  
  geom_jitter(data = nonHighlightLog, aes(PC1, PC2), alpha = 0.05, size = 2)

##########################################################################################
## Data exploration and visualization: Geo-context
##########################################################################################

# try to access or download the data file first
usersCh <- readObjectByName("UsersCH")
# do this only if the file isn't there to avoid costly Google geomapping calls
if (is.null(usersCh) && !file.exists(filePathForObjectName("UsersCH"))) {
  # the environment variable GOOGLE_API_KEY is required or simply copy-paste your
  # google key instead. To obtain a google key, follow the steps outlined here:
  # https://developers.google.com/maps/documentation/javascript/get-api-key
  register_google(key=Sys.getenv("GOOGLE_API_KEY"))
  
  # get users whose location is Switzerland only
  usersCh <- users %>%
    filter(str_detect(tolower(location), 
                      "(\\bch\\b|switzerland|schweiz|svizzera|suisse|svizra)")) %>%
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
# expected number of users located in Switzerland
stopifnot(nrow(usersCh) == 4258)

# get the top answer skills for users located in Switzerland 
topAnswerTags <- answers %>%
  semi_join(usersCh, by="userId") %>%
  group_by(userId) %>%
  summarise(score=max(score)) %>%
  ungroup() %>%
  inner_join(answers %>% select(userId, score, questionId), by=c("userId", "score")) %>%
  inner_join(questions %>% select(questionId, tags), by="questionId") %>%
  group_by(userId) %>%
  summarise(questionId=first(questionId), score=first(score), tags=first(tags)) %>%
  ungroup() %>%
  mutate(type='answer') %>%
  arrange(desc(score))

# testing the results
nrow(topAnswerTags)
topAnswerTags %>%
  head(20)
# double-check results on my userId
topAnswerTags %>% filter(userId == 1142881)

# check for duplicates
topAnswerTags %>%
  group_by(userId) %>%
  filter(n() > 1)

# otherwise get top questions for users located in Switzerland
topQuestionTags <- questions %>%
  semi_join(usersCh, by="userId") %>%
  anti_join(topAnswerTags, by="userId") %>%
  group_by(userId) %>%
  summarise(score=max(score)) %>%
  ungroup() %>%
  inner_join(questions %>% select(userId, score, questionId, tags), by=c("userId", "score")) %>%
  group_by(userId) %>%
  summarise(questionId=first(questionId), score=first(score), tags=first(tags)) %>%
  ungroup() %>%
  mutate(type='question') %>%
  arrange(desc(score))

# testing the results
nrow(topQuestionTags)
topQuestionTags %>%
  head(20)

# check for duplicates
topQuestionTags %>%
  group_by(userId) %>%
  filter(n() > 1)

# merge the two data sets
usersChTop <- topAnswerTags %>%
  bind_rows(topQuestionTags) %>%
  mutate(type=as.factor(type)) %>%
  left_join(usersCh %>% select(userId, location, lon, lat), by="userId") %>%
  separate_rows(tags, sep="\\|") %>%
  rename(skill=tags) %>%
  inner_join(mainSkills, by="skill") %>%
  select(questionId, userId, score, skill, type, location, lon, lat)
str(usersChTop)

# link to the principal component highlights, remove others
usersChTop <- usersChTop %>%
  left_join(highlight %>% select(skill, pc, Technology), by=c("skill")) %>%
  select(questionId, userId, score, skill, Technology, type, location, lon, lat) %>%
  filter(!is.na(Technology) & Technology != technologies[9]) %>%
  arrange(desc(score))
str(usersChTop)

# double-check results on my userId
usersChTop %>% filter(userId == 1142881)

# double-check the top scores
usersChTop %>% 
  arrange(desc(score))

# try to access or download it first
map <- readObjectByName("SwissMap")
# do this only if the file isn't there to avoid costly Google map calls
if (is.null(map) && !file.exists(filePathForObjectName("SwissMap"))) {
  # the environment variable GOOGLE_API_KEY is required or simply copy-paste your
  # google key instead. To obtain a google key, follow the steps outlined here:
  # https://developers.google.com/maps/documentation/javascript/get-api-key
  register_google(key=Sys.getenv("GOOGLE_API_KEY"))
  
  # get Google map of Switzerland
  center <- c(lon = 8.227512, lat = 46.818188)
  map <- get_googlemap(center = center, zoom = 7,
                       color = "bw",
                       maptype = "terrain",
                       style = paste("feature:road|visibility:off&style=element:labels|",
                                     "visibility:off&style=feature:administrative|visibility:on|lightness:60", 
                                     sep=""))
  saveObjectByName(map, "SwissMap")
}

# plot the top technology trends in Geo-context in Switzerland
ggmap(map) + 
  scale_colour_manual(values = colorSpec) +
  geom_point(data=usersChTop, aes(x=lon, y=lat, colour=Technology, size=score), 
             position = position_jitterdodge(jitter.width=0.01, jitter.height=0.01, seed=1)) +
  ggtitle("SO Users located in Switzerland and their matching technology trends, weighted by score") +
  theme(plot.title = element_text(hjust = 0.5), legend.text=element_text(size=12))

# reveal the details of the top ten most prominent data points
usersChTop %>%
  top_n(10, score) %>%
  arrange(desc(score))

##########################################################################################
## Data exploration and visualization: Badges, Answers and Questions
##########################################################################################

# Function to accumulates all badges into a final dataset. Every next call
# should pass the accumulated results so that they can be excluded from 
# the selection.
#
# @param aBadge the badge to filter for. 
# @param acc the accumulated results (result of previous call to this function).
# @param N the top N values to pick e.g. 1500
#
accumulateBadges <- function(aBadge, acc=NULL, N=1500) {
  cat(sprintf("adding \"%s\" to the cumulative badges\n", aBadge))
  res <- NULL
  # handle this non standard case separately
  if (aBadge == "Other Answers") {
    res <- users %>% 
      anti_join(acc %>% select(userId) %>% unique(), by="userId") %>%
      semi_join(answers %>% filter(0 <= score & score < 10) %>% 
                   select(userId) %>% unique(), by="userId") %>%
      mutate(class="bronze", badge=aBadge) %>%
      select(userId, class, badge, reputation) %>%
      bind_rows(acc)
        
  } else {
    # this is the case for the first time
    if (is.null(acc)) {
      res <- users %>% 
        inner_join(badges %>% filter(badge == aBadge) %>% 
                     select(userId, class, badge) %>% unique(), by="userId") %>%
        select(userId, class, badge, reputation)
    } else {
      res <- users %>% 
        anti_join(acc %>% select(userId) %>% unique(), by="userId") %>%
        inner_join(badges %>% filter(badge == aBadge) %>% 
                     select(userId, class, badge) %>% unique(), by="userId") %>%
        select(userId, class, badge, reputation) %>%
        bind_rows(acc)
    }
  }
  # sort by factor ordering
  res$class <- factor(as.character(res$class), levels=c("gold", "silver", "bronze"))
  return(res)
}

# let's check whether the badge system provides qualitatively a significative users' 
# segregation w.r.t reputation using answers and questions. Compare the users by badge
# gold vs silver vs bronze badges.

N <- 1500
badgesOrder <- c("Populist", "Great Answer", "Great Question", "Guru", 
                 "Good Answer", "Nice Answer", "Other Answers", 
                 "Good Question", "Nice Question")

# accumulate all badge selections into a complete set
for (i in 1:length(badgesOrder)) {
  if (i == 1) {
    comp <- accumulateBadges(badgesOrder[i])
  } else {
    comp <- accumulateBadges(badgesOrder[i], comp)
  }
}

# show how many users per class and badge
comp %>% 
  group_by(class, badge) %>%
  summarise(n = n())

# set the seed again
portable.set.seed(1)
# log10 transform reputation and sort by factor ordering
comp <- comp %>%
  filter(reputation > 999) %>%
  group_by(class, badge) %>%
  sample_n(N) %>%
  ungroup() %>%
  mutate(reputation=log10(reputation)) %>%
  mutate(badge = factor(badge, levels=badgesOrder)) 

# checkout the average reputation ordering by badge to get an idea though
# this is not the exact final ordering used due to the exclusion system.
users %>%
  inner_join(badges %>% select(userId, class, badge) %>% unique(), by="userId") %>%
  filter(badge %in% badgesOrder) %>%
  group_by(class, badge) %>%
  summarise(avg_reputation=median(reputation)) %>%
  arrange(desc(avg_reputation))

# create color specification for the different badges
colorSpec <- c("#f9a602", "#c0c0c0", "#cd7f32")
names(colorSpec) <- c("gold", "silver", "bronze")

selectedBadges <- c("Great Answer", "Good Answer", "Nice Answer")
summaryRep <- comp %>%
  group_by(class, badge) %>%
  summarise(median=median(reputation))
comp %>% 
  filter(badge %in% selectedBadges) %>%
  ggplot(aes(reputation, colour = class, fill = class, group = badge)) +
  geom_histogram(position = "dodge", bins = 40) +
  scale_fill_manual(values = colorSpec) +
  xlab("log10 reputation") +
  scale_colour_manual(values = colorSpec) +
  geom_vline(data=summaryRep %>% filter(badge %in% selectedBadges), 
             aes(xintercept=median, color=class), linetype="dashed")

# bootstrapping the median using 2x replications
B <- 2*N

# extend ggplot with a new boxplot stat_summary function to use bootstrapped BCa 
# for the notch confidence intervals
bootNotch <- function(values) {
  # usual quantile values
  res = data.frame(t(boxplot(values, plot=FALSE)$stats))
  colnames(res) = c("ymin","lower","middle","upper","ymax")
  
  # bootstrap and get lower + upper notches
  ci <- boot.ci(boot(values, statistic = function(x, index) median(x[index]), R=B), type="bca")
  res$notchlower = ci$bca[4]
  res$notchupper = ci$bca[5]
  return(res)
}

# since we're extending the boxplot, need to provide outliers calculation as well
outlierNotch <- function(values) {
  return(boxplot(values, plot=FALSE)$out)
}

# compute a summary frame of the data containing mean and median
summaryRep <- comp %>%
  group_by(badge, class) %>%
  summarise(mean=mean(reputation),
            median=median(reputation))
summaryRep

# set the seed again (we need it here to get predictable bootstrap results)
portable.set.seed(1)
# plot the badge & class combinations to match an ordering for the ratings
comp %>% 
  left_join(summaryRep, by=c("class", "badge")) %>%
  ggplot(aes(x=badge, y=reputation, colour=class, group=badge)) +
  ggtitle("Reputation for random samples of 1.5k users in each badge & class combination") +
  ylab(label = "log10 reputation") +
  theme(legend.position="bottom", plot.title = element_text(hjust = 0.5),
        legend.text=element_text(size=12), 
        axis.text.x = element_text(angle = 45, hjust = 1)) + 
  stat_summary(fun.data = bootNotch, geom = "boxplot", notch = T) +
  stat_summary(fun.y = outlierNotch, geom = "point") +
  stat_summary(fun.y = mean, geom = "point", shape = 20, size = 5) +
  scale_colour_manual(values = colorSpec) +
  geom_hline(data=summaryRep, aes(yintercept = median, color = class), 
             linetype = "dashed") +
  geom_jitter(alpha=0.05)

# generate all possible pair-wise rating combinations
c <- combn(badgesOrder, m=2)
# run Wilcoxon rank sum test for independent samples i.e. significance of 
# the difference in population median between non-normally distributed 
# unpaired groups. Print the pairs whose p-value is greater than 0.05 i.e. 
# the mean difference between groups is not significative at
# 95% confidence
for (i in 1:ncol(c)) {
  res <- comp %>%
    filter(badge == c[1,i] | badge == c[2,i]) %>%
    wilcox.test(reputation~badge, data=., paired=F, conf.int=T)
  if (res$p.value > 0.05) {
    cat(sprintf(paste0("The median rep. for users with badge ",
                       "'%s' and '%s' is NOT significatively ", 
                       "diff. with p-value=%.6f\n"), 
                c[1,i], c[2,i], res$p.value))
  }
}

##########################################################################################
## Load and explore the ratings dataset.
##########################################################################################

# free a bit of memory if possible
gc(TRUE, TRUE, TRUE)

# read the ratings and users dataset
ratings <- readObjectByName("Ratings")

# check the average
ratings %>%
  summarise(median = median(rating), mean = mean(rating), sd = sd(rating))

# checkout the ratings histogram, it's bell shaped
ratings %>% 
  ggplot(aes(rating, fill=..x..)) + geom_histogram() +
  scale_x_continuous(breaks = seq(1.5, 5, by=0.5)) +
  scale_fill_gradient("Legend", low = "#E41A1C", high = "#4DAF4A") +
  theme(legend.position="bottom", legend.title = element_blank())

# set the seed again
portable.set.seed(1)
# repeat the analysis after having the ratings, use BCa again
comp <- ratings %>%  
  left_join(users %>% select(userId, reputation) %>% unique(), by="userId") %>%
  mutate(reputation=log10(reputation)) %>%
  group_by(rating) %>%
  sample_n(N) %>%
  ungroup()

# compute data summary
summaryRep <- comp %>%
  group_by(rating) %>%
  summarise(mean=mean(reputation),
            median=median(reputation))

# set the seed again
portable.set.seed(1)
comp %>%
  ggplot(aes(x=rating, y=reputation, colour=rating, group=rating)) +
  ggtitle("Top 1.5k unique users in each rating group") +
  ylab(label = "log10 reputation") +
  theme(legend.position="bottom", plot.title = element_text(hjust = 0.5),
        legend.text=element_text(size=12), 
        axis.text.x = element_text(angle = 45, hjust = 1)) + 
  stat_summary(fun.data = bootNotch, geom = "boxplot", notch = T) +
  stat_summary(fun.y = outlierNotch, geom = "point") +
  stat_summary(fun.y = mean, geom = "point", shape = 20, size = 5) +
  scale_color_gradient("Legend", low = "#E41A1C", high = "#4DAF4A") +
  geom_hline(data=summaryRep, aes(yintercept = median, color = rating), 
             linetype = "dashed", alpha=0.5) +
  geom_jitter(alpha=0.1)

# generate all possible pair-wise rating combinations
c <- combn(seq(1.5, 5.0, by=0.5), m=2)
# run Wilcoxon rank sum test for independent samples i.e. significance of 
# the difference in population median between non-normally distributed 
# unpaired groups. Print the pairs whose p-value is greater than 0.05 i.e. 
# the mean difference between groups is not significative at
# 95% confidence
for (i in 1:ncol(c)) {
  res <- comp %>%
    filter(rating == c[1,i] | rating == c[2,i]) %>%
    wilcox.test(reputation~rating, data=., paired=F, conf.int=T)
  if (res$p.value > 0.05) {
    cat(sprintf(paste0("The median rep. for users with rating ",
                       "%.1f and %.1f is NOT significatively ", 
                       "diff. with p-value=%.6f\n"), 
                c[1,i], c[2,i], res$p.value))
  }
}

##########################################################################################
## Split the ratings dataset into training and test sets.
##########################################################################################

# we shouldn't need these anymore
rm(users, questions, answers, badges, tags)

# free a bit of memory if possible
gc(TRUE, TRUE, TRUE)

# what's the number of unique users, skills and how sparse is it?
ratings %>% 
  summarise(users = n_distinct(userId), 
            skills = n_distinct(skill),
            n = n()) %>%
  mutate(sparsity=sprintf("%.1f%%", 100*n / (users*skills))) %>%
  select(users, skills, sparsity)

# split the ratings dataset into separate train and test sets
portable.set.seed(1)
testIndex <- createDataPartition(y = ratings$rating, times = 1,
                                 p = 0.1, list = FALSE)
trainSet <- ratings[-testIndex,]
tmp <- ratings[testIndex,]

# make sure userId and skill in test set are also in train set
testSet <- tmp %>%
  semi_join(trainSet %>% select(userId) %>% unique(), by="userId") %>%
  semi_join(trainSet %>% select(skill) %>% unique(), by="skill")
testSet

# add rows removed from test set back into the train set
removed <- tmp %>%
  anti_join(testSet, by=c("userId", "skill"))

trainSet <- trainSet %>% 
  bind_rows(removed)
rm(tmp, removed)

# test the results, the two sets must add up
stopifnot(nrow(trainSet) + nrow(testSet) == nrow(ratings))

##########################################################################################
## Explore the simpler baseline model first.
##########################################################################################

# global average
mu <- mean(trainSet$rating)
# compute predictions and RMSE
rmseResults <- tibble(method = "Just the average", RMSE = Metrics::rmse(trainSet$rating, mu))
rmseResults

lambda <- 4
# compute regularized user effects
userEffects <- trainSet %>%
  group_by(userId) %>%
  summarize(b_i = sum(rating - mu)/(n() + lambda))
# compute predictions and RMSE
predictedRatings <- trainSet %>%
  left_join(userEffects, by='userId') %>%
  mutate(pred=mu + b_i) %>%
  pull(pred)
rmseResults <- bind_rows(rmseResults,
                         tibble(method="Regularized User Effects",
                                RMSE = Metrics::rmse(predictedRatings, trainSet$rating)))
rmseResults

# compute regularized skill effects
skillEffects <- trainSet %>%
  left_join(userEffects, by='userId') %>%
  group_by(skill) %>%
  summarize(b_j = sum(rating - (mu + b_i))/(n() + lambda))
# compute predictions and RMSE
predictedRatings <- trainSet %>%
  left_join(userEffects, by='userId') %>%
  left_join(skillEffects, by='skill') %>%
  mutate(pred=mu + b_i + b_j) %>%
  pull(pred)
rmseResults <- bind_rows(rmseResults,
                         tibble(method="Regularized User + Skill Effects",
                                 RMSE = Metrics::rmse(predictedRatings, trainSet$rating)))
rmseResults

# show the effects of number of week blocks since first post 
weeksBlock <- 30
# this week blocks corresponds approximately to 7 months
round(weeksBlock / 4.34524)
# show the gaining experience over time effects 
trainSet %>%
  left_join(skillEffects, by='skill') %>%
  left_join(userEffects, by='userId') %>%
  mutate(residual=rating - (mu + b_i + b_j)) %>%
  mutate(week_block_30 = ceiling(as.duration(firstPostDate %--% creationDate) / dweeks(weeksBlock))) %>%
  arrange(desc(week_block_30)) %>%
  group_by(week_block_30) %>%
  summarise(b_e_Effect=mean(residual)) %>%
  ggplot(aes(week_block_30, b_e_Effect)) + geom_point() + 
  geom_smooth(color="red", span=0.3, method.args=list(degree=2))

# fit a loess smoothing to model the "experience gained" temporal effect
weeksBlockFit <- trainSet %>%
  left_join(userEffects, by='userId') %>%
  left_join(skillEffects, by='skill') %>%
  mutate(residual=rating - (mu + b_i + b_j)) %>%
  mutate(week = ceiling(as.duration(firstPostDate %--% creationDate) / dweeks(weeksBlock))) %>%
  group_by(week) %>%
  summarise(residual=mean(residual)) %>%
  loess(residual~week, data=., span=0.3, degree=2)

# compute predictions and RMSE
predictedRatings <- trainSet %>%
  left_join(userEffects, by='userId') %>%
  left_join(skillEffects, by='skill') %>%
  mutate(week = ceiling(as.duration(firstPostDate %--% creationDate) / dweeks(weeksBlock))) %>%
  mutate(pred=mu + b_i + b_j + predict(weeksBlockFit, .)) %>%
  pull(pred)
rmseResults <- bind_rows(rmseResults,
                         tibble(method="Regularized User + Skill + Experience Effects",
                                RMSE = Metrics::rmse(predictedRatings, trainSet$rating)))
rmseResults

##########################################################################################
## Compute the RMSE for the baseline model on the test set.
##########################################################################################

## TEST SET ACCESS ALERT! accessing the test set to compute RMSE.
predictedRatings <- testSet %>%
  left_join(userEffects, by='userId') %>%
  left_join(skillEffects, by='skill') %>%
  mutate(week = ceiling(as.duration(firstPostDate %--% creationDate) / dweeks(weeksBlock))) %>%
  mutate(pred=mu + b_i + b_j + predict(weeksBlockFit, .)) %>%
  pull(pred)
rmseValue <- Metrics::rmse(predictedRatings, testSet$rating)
cat(sprintf("baseline RMSE on test data is %.9f\n", rmseValue))
# check that we get reproducible results
stopifnot(abs(rmseValue - 0.653410415) < 1e-9)

##########################################################################################
## Create the CF Low-Rank Matrix Factorization (lrmf) model integrated with the caret 
## package. The model employs low-rank matrix factorization trained using SGD.
##
## This caret model specification follows the implementation details described here:
## https://topepo.github.io/caret/using-your-own-model-in-train.html
##########################################################################################

# Define the model lrmf (Collaborative Filtering Low-Rank Matrix Factorization)
lrmf <- list(type = "Regression",
             library = NULL,
             loop = NULL,
             prob = NULL,
             sort = NULL)

# Define the model parameters. Four different parameters are supported.
#
# @param K the number of latent dimensions
# @param maxGamma the maximum learning rate.
# @param lambda the regularizaton parameter applied to the different effects.
# @param sigma the standard deviation of the initial values.
#
lrmf$parameters <- data.frame(parameter = c("K", "maxGamma", "lambda", "sigma"),
                              class = c(rep("numeric", 4)),
                              label = c("K-Latent dimensions", "Max. Learning rate", "Lambda", "Sigma of initial values"))

# Define the required grid function, which is used to create the tuning grid (unless the user 
# gives the exact values of the parameters via tuneGrid)
lrmf$grid <- function(x, y, len = NULL, search = "grid") {
  K <- c(10, 11, 12)
  maxGamma <- c(0.05, 0.1, 0.11)
  lambda <- seq(0.05, 0.1, by=0.01)
  sigma <- c(0.05, 0.1, 0.15)
  
  # to use grid search
  out <- expand.grid(K = K,
                     maxGamma = maxGamma,
                     lambda = lambda,
                     sigma = sigma)
  
  if(search == "random") {
    # random search simply random samples from the expanded grid
    out <- out %>%
      sample_n(100)
  }
  return(out)
}

# Define the fit function so we can fit our model to the data.
#
# @param P the initial P matrix.
# @param Q the initial Q matrix.
# @param batchSize the number of samples to train with at every step.
# @param trackConv whether to track RMSE convergence of the algorithm.
# @param thresRatio threshold for the ratio of initial gamma to gamma.
# @param iterBreaks number of steps before tracking RMSE convergence.
# @param verbose whether to output extra convergence information.
# @param ncores the number of cores to use for parallel batches.
# @param maxIter maximum number of outer iterations.
# @param batchIter number of batch iterations of the SGD algorithm.
#
lrmf$fit <- function(x, y, wts, param, lev, last, weights, classProbs, P=NULL, Q=NULL, 
                     batchSize=1000*param$K, trackConv=F, perTrack=1, thresRatio=1000, 
                     iterBreaks=1, verbose=F, ncores=detectCores(), maxIter=250,
                     batchIter=ifelse(ncores == 1, 108, ncores^2*3), ...) {
  # check whether we have a correct x
  stopifnot("userId" %in% colnames(x))
  stopifnot("skill"  %in% colnames(x))
  stopifnot("rating" %in% colnames(x))
  stopifnot(all(x$rating == y))
  
  # save some parameters
  gamma  <- param$maxGamma                     # learning rate
  lambda <- param$lambda                       # regularization parameter
  K <- param$K                                 # number of latent dimensions
  N <- nrow(x %>% select(userId) %>% unique()) # number of users
  M <- nrow(x %>% select(skill)  %>% unique()) # number of skills
  
  # compute and save the global mean and sd
  globalMu    <- mean(x$rating)
  globalSigma <- sd  (x$rating)
  
  # compute the z-score
  x <- x %>% select(userId, skill, rating) %>%
    mutate(rating_z = (rating - globalMu) / globalSigma)

  # compute the user and skill effects
  skillEffects <- x %>% group_by(skill)  %>% summarise(b_s = mean(rating_z))
  userEffects  <- x %>% group_by(userId) %>% summarise(b_u = mean(rating_z))

  # indexing for users and skills
  userIndex  <- x %>% distinct(userId) %>% arrange(userId) %>% mutate(i = row_number())
  skillIndex <- x %>% distinct(skill)  %>% arrange(skill)  %>% mutate(j = row_number())
  
  # create the actual x
  x <- x %>%
    left_join(userIndex , by="userId") %>%
    left_join(skillIndex, by="skill") %>%
    select(i, j, rating, rating_z)

  # initialize P and Q to have the skill and user effects already encoded in
  # the columns are layout so that the computation is column-major aligned 
  if (is.null(P) || is.null(Q)) {
    P <- matrix(0, nrow = K, ncol = N)
    P[1,] <- matrix(1, nrow = 1, ncol = N)
    P[2,] <- as.matrix(userIndex %>% left_join(userEffects, by="userId") %>% select(b_u))
    P <- P + matrix(rnorm(K*N, mean = 0, sd = param$sigma), nrow = K, ncol = N)

    Q <- matrix(0, nrow = K, ncol = M)
    Q[1,] <- as.matrix(skillIndex %>% left_join(skillEffects, by="skill") %>% select(b_s))
    Q[2,] <- matrix(1, nrow = 1, ncol = M)
    Q <- Q + matrix(rnorm(K*M, mean = 0, sd = param$sigma), nrow = K, ncol = M)
  }

  # double-check the matrix dimensions
  stopifnot(nrow(P) == K)
  stopifnot(ncol(P) == N)
  stopifnot(nrow(Q) == K)
  stopifnot(ncol(Q) == M)
  
  # convenience function to compute the RMSE for a subset of the samples
  computeRMSE <- function(subsetSamples) {
    predicted <- globalMu + globalSigma*colSums(P[,subsetSamples$i]*Q[,subsetSamples$j])
    return(Metrics::rmse(predicted, subsetSamples$rating))
  }
  
  # prepare for parallel computation, blocks contain ncores number of groups
  # that can be each executed in parallel, each group trains a mutually
  # exclusive permutation of the training data tuples
  if (ncores > 1) {
    if (verbose) {
      cat(sprintf('running parallel version with %d cores\n', ncores))
    }
    bi <- rep(0:(ncores-1), ncores)
    bj <- (bi + rep(0:(ncores-1), 1, each = ncores)) %% ncores
    blocks <- tibble(bi=bi, bj=bj)
    
    # how many iterations each core will do
    parIter <- batchIter / ncores
    
    # include the block indexes bi and bj for parallel processing
    x <- x %>%
      mutate(bi = i %% ncores, bj = j %% ncores)
    
    # correct the batchSize if needed, the lower bound is the minimum
    # number of elements within each group
    lowerBound <- x %>% 
      group_by(bi, bj) %>% 
      summarise(n=n()) %>% 
      ungroup() %>%
      summarise(n=min(n)) %>% 
      pull(n)
    if (lowerBound < batchSize) {
      batchSize <- lowerBound
      if (verbose) {
        cat(sprintf('corrected the batch size to %d\n', batchSize))
      }
    }

  } else {
    if (verbose) {
      cat(sprintf('running sequential version\n', ncores))
    }
  }
  
  # track convergence on these samples
  subsetSamples <- x %>% sample_n(nrow(x)*perTrack)
  rmseValue <- computeRMSE(subsetSamples)
  if (verbose) {
    cat(sprintf('the training RMSE at iter=0 is %.9f\n', rmseValue))
  }
  
  if (trackConv) {
    rmseHist <- tibble(iter=0, K=K, rmse=rmseValue)
  } else {
    rmseHist <- NULL
  }
  
  for (iter in 1:maxIter) {
    # for performance reasons the SGD update code is duplicated in both 
    # implementations; using a function would defeat the purpose due to 
    # R's pass-by-value and the undesirable cost on copying the matrices
    if (ncores == 1) {
      for (iter2 in 1:batchIter) {
        # choose a random batch of samples
        samples <- x %>% sample_n(batchSize)
        
        # get hold of the indexes
        i <- samples$i
        j <- samples$j
        
        # compute the residuals
        epsilon <- samples$rating_z - colSums(P[,i]*Q[,j])
        
        # partial derivatives w.r.t. P and Q
        P_upd <- (P[,i] + gamma*(Q[,j]*epsilon[col(Q[,j])] - lambda*P[,i]))
        Q[,j] <- (Q[,j] + gamma*(P[,i]*epsilon[col(P[,i])] - lambda*Q[,j]))
        P[,i] <- P_upd
      }
    } else {
      stopifnot(nrow(blocks) == ncores^2)
      
      # pick a random block group
      g <- sample(0:(ncores-1), 1)

      # process the selected block group
      res <- mclapply((g*ncores + 1):((g + 1)*ncores), mc.cores = ncores, mc.set.seed = TRUE, 
        FUN = function(b) {
          # select the subset of samples corresponding to this block
          blockSamples <- x %>%
            filter(bi == blocks[b,]$bi & bj == blocks[b,]$bj)
          stopifnot(nrow(blockSamples) > 0)
          
          # keep track of the updated columns
          ii <- NULL
          jj <- NULL
          
          # run multiple batches on this block
          for (iter2 in 0:(parIter-1)) {
            samples <- blockSamples %>% sample_n(batchSize)

            # get hold of the indexes
            i <- samples$i
            j <- samples$j

            # compute the residuals
            epsilon <- samples$rating_z - colSums(P[,i]*Q[,j])
            
            # partial derivatives w.r.t. P and Q
            P_upd <- (P[,i] + gamma*(Q[,j]*epsilon[col(Q[,j])] - lambda*P[,i]))
            Q[,j] <- (Q[,j] + gamma*(P[,i]*epsilon[col(P[,i])] - lambda*Q[,j]))
            P[,i] <- P_upd
            
            # accumulate the changed indexes
            if (is.null(ii)) {
              ii <- i
            } else {
              ii <- c(ii, i)
            }
            
            if (is.null(jj)) {
              jj <- j
            } else {
              jj <- c(jj, j)
            }
          }
          
          ii <- sort(unique(ii))
          jj <- sort(unique(jj))
          
          # output the updated user and skill columns
          return(list(Pii=P[,ii],Qjj=Q[,jj],ii=ii,jj=jj))
        })
      
      # consolidate updates into the P and Q matrices
      for (k in 1:length(res)) {
        l <- res[[k]]
        P[,l$ii] <- l$Pii
        Q[,l$jj] <- l$Qjj
      }      
    }
    
    # check rmse
    rmsePrevious <- rmseValue
    rmseValue    <- computeRMSE(subsetSamples)

    # track convergence at a number of steps
    if (trackConv && iter %% iterBreaks == 0) {
      if (verbose) {
        cat(sprintf('the training RMSE at iter=%d is %.9f\n', iter, rmseValue))
      }
      rmseHist <- rmseHist %>% 
        add_row(iter=iter, K=K, rmse=rmseValue)
    }

    # check whether the rmse improved, if not then halve gamma
    if (rmsePrevious < rmseValue) {
      gamma <- gamma / 2
      if (verbose) {
        cat(sprintf("decreased the learning rate to: %.9f\n", gamma))
      }
    } else {
      if (gamma < param$maxGamma) {
        # increase the learning rate more slowly 
        gamma <- min(param$maxGamma, gamma*3/2)
        if (verbose) {
          cat(sprintf("increased the learning rate to: %.9f\n", gamma))
        }
      }
    }

    # if threshold ratio exceeded then bounce gamma back to previous value
    if (param$maxGamma / gamma > thresRatio) {
      gamma <- gamma*2
      if (verbose) {
        cat(sprintf("bounced the learning rate to: %.9f\n", gamma))
      }
    }
  }

  # return the model fit as a list
  return(list(globalMu=globalMu,
              globalSigma=globalSigma,
              skillEffects=skillEffects,
              userEffects=userEffects,
              userIndex=userIndex,
              skillIndex=skillIndex,
              P=P,
              Q=Q,
              rmseHist=rmseHist,
              params=param))
}

# Define the predict function that produces a vector of predictions
lrmf$predict <- function(modelFit, newdata, preProc = NULL, submodels = NULL) {
  # check whether we have a correct newdata
  stopifnot("userId" %in% colnames(newdata))
  stopifnot("skill"  %in% colnames(newdata))
  
  # link the newdata to the indexes per user and skill
  newdata <- newdata %>% 
    left_join(modelFit$userIndex , by="userId") %>%
    left_join(modelFit$skillIndex, by="skill") %>%
    select(i, j)
  
  predicted <- modelFit$globalMu + modelFit$globalSigma*
    colSums(modelFit$P[,newdata$i]*modelFit$Q[,newdata$j])
  return(predicted)
}

##########################################################################################
## Build a representative calibration (cross validation) subset of the training set.
##########################################################################################

N <- 240
tic(sprintf("preparing a calibration set of %d random users", N))
# set the seed again
portable.set.seed(1)
usersSel <- trainSet %>% 
  group_by(rating) %>%
  select(userId, rating) %>% 
  unique() %>% 
  sample_n(N / 8)
# all ratings for those users
calibrationSet <- trainSet %>%
  semi_join(usersSel, by="userId")
toc()
rm(usersSel)

# how many rows, distinct skills and users in the calibration set?
cat(sprintf("The calibration set contains %d rows, %d unique users and %d skills\n", 
            nrow(calibrationSet),
            length(unique(calibrationSet$userId)), 
            length(unique(calibrationSet$skill))))

# check the ratings distribution of the calibration set
calibrationSet %>% 
  group_by(rating) %>% 
  count()

calibrationSet %>% 
  group_by(skill) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

calibrationSet %>% 
  group_by(userId) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

##########################################################################################
## Calibrate the "lrmf" model on the calibration set (subset of the training set). Here 
## we look for the best hyper-parameters that fit the model on a small subset of the 
## training data.
##########################################################################################

tic('calibrating the LRMF model')
# set the seed again
portable.set.seed(1)
control <- trainControl(method = "cv",
                        search = "grid",
                        number = 10,        # use 10 K-folds in cross validation
                        p = .9,             # use 90% of training and 10% for testing
                        allowParallel = T,  # execute CV folds in parallel
                        verboseIter = T)
cvFit <- train(x = calibrationSet,
               y = calibrationSet$rating,
               method = lrmf,
               trControl = control,
               ncores = 1,
               maxIter = 100,
               batchIter = 50,
               batchSize = 100)
toc()

## The bestTune model found is:
stopifnot(cvFit$bestTune$K == 11)
stopifnot(cvFit$bestTune$maxGamma == 0.05)
stopifnot(cvFit$bestTune$lambda == 0.1)
stopifnot(cvFit$bestTune$sigma == 0.15)

##########################################################################################
## Fit the best model found to the complete training set.
##########################################################################################

tic('training LRMF on the full training set - parallel')
# set the seed again
portable.set.seed(1)
fitPar <- train(x = trainSet,
                y = trainSet$rating,
                method = lrmf,
                trControl = trainControl(method = "none"),
                tuneGrid = cvFit$bestTune,
                trackConv = T,
                iterBreaks = 1,
                verbose = F,
                ncores = ncores, 
                maxIter = 125,
                batchIter = 216)
ticTocTimes <- toc()
elapsedPar <- ticTocTimes$toc[[1]] - ticTocTimes$tic[[1]]

# save the RMSE history
rmseHist <- fitPar$finalModel$rmseHist %>% 
  add_column(method=sprintf("Parallel - %d cores", ncores))

tic('training LRMF on the full training set - classic')
# set the seed again
portable.set.seed(1)
fitSeq <- train(x = trainSet,
                y = trainSet$rating,
                method = lrmf,
                trControl = trainControl(method = "none"),
                tuneGrid = cvFit$bestTune,
                trackConv = T,
                iterBreaks = 1,
                verbose = F,
                ncores = 1, 
                maxIter = 250,
                batchIter = 108)
ticTocTimes <- toc()
elapsedSeq <- ticTocTimes$toc[[1]] - ticTocTimes$tic[[1]]

# save the RMSE history
rmseHist <- rmseHist %>%
  bind_rows(fitSeq$finalModel$rmseHist %>% 
              add_column(method="Classic"))

# plot the convergence for the model on the full training data
colorSpec <- c("salmon", "turquoise3")
names(colorSpec) <- c("Classic", sprintf("Parallel - %d cores", ncores))
rmseHist %>%
  ggplot(aes(iter, rmse, color=method, group=method)) + 
  geom_point(aes(shape=method), size=2) + 
  geom_line() +
  scale_colour_manual(values = colorSpec) +  
  theme(plot.title = element_text(hjust = 0.5), legend.text=element_text(size=12)) + 
  xlab("Iterations") + ylab("RMSE") +
  annotate("text", x = 125, colour = colorSpec[2], 
           y = rmseHist %>% 
             filter(method == sprintf("Parallel - %d cores", ncores)) %>% 
             last() %>% 
             pull(rmse) - 0.004,
           label = sprintf("%.2f sec", elapsedPar)) + 
  annotate("text", x = 250, colour = colorSpec[1],
           y = rmseHist %>% 
             filter(method == "Classic") %>% 
             last() %>% 
             pull(rmse) - 0.004, 
           label = sprintf("%.2f sec", elapsedSeq)) + 
  ggtitle("Users-skills rating prediction using LRMF - Parallel vs. classic method")

##########################################################################################
## Compute the RMSE for the model on the test set.
##########################################################################################

## TEST SET ACCESS ALERT! accessing the test set to compute RMSE.
predictedRatings <- predict(fitPar, testSet)
rmseValue <- Metrics::rmse(predictedRatings, testSet$rating)
cat(sprintf("RMSE on test data is %.9f\n", rmseValue))
# check that we get reproducible results
stopifnot(abs(rmseValue - 0.659181944) < 1e-9)

## TEST SET ACCESS ALERT! accessing the test set to compute RMSE.
predictedRatings <- predict(fitSeq, testSet)
rmseValue <- Metrics::rmse(predictedRatings, testSet$rating)
cat(sprintf("RMSE on test data is %.9f\n", rmseValue))
# check that we get reproducible results
stopifnot(abs(rmseValue - 0.650566815) < 1e-9)

# using only ~0.5% of the ratings data
percData <- 100*(125*216)/nrow(ratings)
percData
stopifnot(abs(percData - 0.4960501) < 1e-7)

##########################################################################################
## Finally predict how good I'd be predicted to be on skills for which there is no evidence.
##########################################################################################

# where is the author top rated?
ratings %>% 
  filter(userId == 1142881) %>%
  arrange(desc(rating))

# find the skills for which I have no ratings i.e. there is no evidence
noEvidenceSkills <- mainSkills %>%
  anti_join(ratings %>% filter(userId == 1142881) %>% select(skill) %>% unique(), by="skill") %>%
  arrange(desc(count))
noEvidenceSkills

# compute the ratings average for each of those skills
avgNoEvidenceSkills <- ratings %>%
  group_by(skill) %>%
  summarise(avg=mean(rating)) %>%
  semi_join(noEvidenceSkills, by="skill")
avgNoEvidenceSkills

# create new data for prediction
newdata <- noEvidenceSkills %>%
  select(skill, count) %>%
  mutate(userId=1142881) %>%
  inner_join(avgNoEvidenceSkills, by="skill") %>%
  select(userId, skill, count, avg)

# compute skill rating predictions
newdata$predicted <- predict(fitSeq, newdata)

# show how would be rated for the following technologies
newdata %>% 
  filter(skill %in% c("tableau", "google-maps", "c++11", "c++17", 
                      "ejb", "java-stream", "teradata", "itext",
                      "blockchain", "apache-kafka", "haskell", "go")) %>%
  arrange(desc(predicted))

# show the top 20 skills where the predicted rating is above average
newdata %>% 
  filter(predicted > avg) %>% 
  top_n(20, predicted) %>%
  arrange(desc(predicted))