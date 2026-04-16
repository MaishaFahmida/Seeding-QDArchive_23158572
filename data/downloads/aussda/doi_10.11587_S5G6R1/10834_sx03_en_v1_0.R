source("10834_sx01_en_v1_0.R")

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 010. Data Import----------
#-#-#-#-#-#-#-#-#-#-#-#-#-#

p_load(readtext,    # read files
       lubridate,   # work with date-times and time-spans
       zoo,         # convert dates
       tidyverse    # data manipulation and plotting
       )

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# Data Foreign Ministry----
#-#-#-#-#-#-#-#-#-#-#-#-#-#

# read-in press releases of Cabinet Kern

# for mac users
dat_POP1 <- readtext("./data/ots/Bundesministerium_europ_int_Angelegenheiten_17052016_17122017.pdf",
                     encoding = "UTF8")

# for windows users
# dat_POP1 <- readtext("./10834_da_de_v1_0/data/ots/Bundesministerium_europ_int_Angelegenheiten_17052016_17122017.pdf",
#                     encoding = "UTF8")


x <- str_extract_all(dat_POP1$text, "(OTS[0-9]{4})((.|\n)*?)([[:digit:]]{6} [[:alpha:]]{3} [[:digit:]]{2})")
dat_POP1 <- as_tibble(x, .name_repair = "unique")
colnames(dat_POP1) <- "pressReleases"
dat_POP1$pressReleases <- str_replace_all(dat_POP1$pressReleases, "Jän ", "Jan ") # change Austrian "Jän" (i.e., "Jänner") to "Jan" (i.e., "January")
dat_POP1$pressReleases <- str_replace_all(dat_POP1$pressReleases, "Mär ", "Mar ") # change Austrian "Mär" (i.e., "März") to "Mar" (i.e., "March")
dat_POP1$pressReleases <- str_replace_all(dat_POP1$pressReleases, "Mai ", "May ") # change Austrian "Mai" (i.e., "Mai") to "May" (i.e., "May")
dat_POP1$pressReleases <- str_replace_all(dat_POP1$pressReleases, "Okt ", "Oct ") # change Austrian "Oktober" (i.e., "Okt") to "Oct" (i.e., "October")
dat_POP1$pressReleases <- str_replace_all(dat_POP1$pressReleases, "Dez ", "Dec ") # change Austrian "Dez" (i.e., "Dezember") to "Dec" (i.e., "December")

dat_POP1 <- dat_POP1 %>% 
  mutate(date = str_extract(dat_POP1$pressReleases, "[[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4}")) %>% 
  mutate(ots = str_extract(dat_POP1$pressReleases, "OTS[0-9]{4}")) %>% 
  mutate(text = str_extract(dat_POP1$pressReleases, "([[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4})((.|\n)*?)(~)")) %>% 
  mutate(cabinet = "Kern") %>%  # Cabinet Kern 
  mutate(ministry = "FMEIA") %>% # Federal Ministry of European and International Affairs
  select(-pressReleases)

dat_POP1$date <- dmy(dat_POP1$date) # transform date to date format
dat_POP1$text <- gsub("\n", " ", dat_POP1$text) # remove /n from text


# read-in press releases of Cabinet Kurz I

# for mac users
dat_POP2 <- readtext("./data/ots/Bundesministerium_europ_int_Angelegenheiten_18122017_28052019.pdf",
                     encoding = "UTF8")

# for windows users
# dat_POP2 <- readtext("./10834_da_de_v1_0/data/ots/Bundesministerium_europ_int_Angelegenheiten_18122017_28052019.pdf",
#                      encoding = "UTF8")


x <- str_extract_all(dat_POP2$text, "(OTS[0-9]{4})((.|\n)*?)([[:digit:]]{6} [[:alpha:]]{3} [[:digit:]]{2})")
dat_POP2 <- as_tibble(x, .name_repair = "unique")
colnames(dat_POP2) <- "pressReleases"
dat_POP2$pressReleases <- str_replace_all(dat_POP2$pressReleases, "Jän ", "Jan ") # change Austrian "Jän" (i.e., "Jänner") to "Jan" (i.e., "January")
dat_POP2$pressReleases <- str_replace_all(dat_POP2$pressReleases, "Mär ", "Mar ") # change Austrian "Mär" (i.e., "März") to "Mar" (i.e., "March")
dat_POP2$pressReleases <- str_replace_all(dat_POP2$pressReleases, "Mai ", "May ") # change Austrian "Mai" (i.e., "Mai") to "May" (i.e., "May")
dat_POP2$pressReleases <- str_replace_all(dat_POP2$pressReleases, "Okt ", "Oct ") # change Austrian "Oktober" (i.e., "Okt") to "Oct" (i.e., "October")
dat_POP2$pressReleases <- str_replace_all(dat_POP2$pressReleases, "Dez ", "Dec ") # change Austrian "Dez" (i.e., "Dezember") to "Dec" (i.e., "December")

dat_POP2 <- dat_POP2 %>% 
  mutate(date = str_extract(dat_POP2$pressReleases, "[[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4}")) %>% 
  mutate(ots = str_extract(dat_POP2$pressReleases, "OTS[0-9]{4}")) %>% 
  mutate(text = str_extract(dat_POP2$pressReleases, "([[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4})((.|\n)*?)(~)")) %>% 
  mutate(cabinet = "KurzI") %>%  # Cabinet Kurz I
  mutate(ministry = "FMEIA") %>% # Federal Ministry of European and International Affairs
  select(-pressReleases)

dat_POP2$date <- dmy(dat_POP2$date) # transform date to date format
dat_POP2$text <- gsub("\n", " ", dat_POP2$text) # remove /n from text


# read-in press releases of Cabinet Kurz II

# for mac users
dat_POP3 <- readtext("./data/ots/Bundesministerium_europ_int_Angelegenheiten_07012020_11102021.pdf",
                     encoding = "UTF8")

# for windows users
# dat_POP3 <- readtext("./10834_da_de_v1_0/data/ots/Bundesministerium_europ_int_Angelegenheiten_07012020_11102021.pdf",
#                      encoding = "UTF8")

x <- str_extract_all(dat_POP3$text, "(OTS[0-9]{4})((.|\n)*?)([[:digit:]]{6} [[:alpha:]]{3} [[:digit:]]{2})")
dat_POP3 <- as_tibble(x, .name_repair = "unique")
colnames(dat_POP3) <- "pressReleases"
dat_POP3$pressReleases <- str_replace_all(dat_POP3$pressReleases, "Jän ", "Jan ") # change Austrian "Jän" (i.e., "Jänner") to "Jan" (i.e., "January")
dat_POP3$pressReleases <- str_replace_all(dat_POP3$pressReleases, "Mär ", "Mar ") # change Austrian "Mär" (i.e., "März") to "Mar" (i.e., "March")
dat_POP3$pressReleases <- str_replace_all(dat_POP3$pressReleases, "Mai ", "May ") # change Austrian "Mai" (i.e., "Mai") to "May" (i.e., "May")
dat_POP3$pressReleases <- str_replace_all(dat_POP3$pressReleases, "Okt ", "Oct ") # change Austrian "Oktober" (i.e., "Okt") to "Oct" (i.e., "October")
dat_POP3$pressReleases <- str_replace_all(dat_POP3$pressReleases, "Dez ", "Dec ") # change Austrian "Dez" (i.e., "Dezember") to "Dec" (i.e., "December")

dat_POP3 <- dat_POP3 %>% 
  mutate(date = str_extract(dat_POP3$pressReleases, "[[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4}")) %>% 
  mutate(ots = str_extract(dat_POP3$pressReleases, "OTS[0-9]{4}")) %>% 
  mutate(text = str_extract(dat_POP3$pressReleases, "([[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4})((.|\n)*?)(~)")) %>% 
  mutate(cabinet = "KurzII") %>%  # Cabinet Kurz II
  mutate(ministry = "FMEIA") %>% # Federal Ministry of European and International Affairs
  select(-pressReleases)

dat_POP3$date <- dmy(dat_POP3$date) # transform date to date format
dat_POP3$text <- gsub("\n", " ", dat_POP3$text) # remove /n from text

# read-in press releases of Cabinets Schallenberg/Nehammer

# for mac users
dat_POP4 <- readtext("./data/ots/Bundesministerium_europ_int_Angelegenheiten_12102021-31072022.pdf",
                     encoding = "UTF8")

# for windows users
# dat_POP4 <- readtext("./10834_da_de_v1_0/data/ots/Bundesministerium_europ_int_Angelegenheiten_12102021-31072022.pdf",
#                      encoding = "UTF8")

x <- str_extract_all(dat_POP4$text, "(OTS[0-9]{4})((.|\n)*?)([[:digit:]]{6} [[:alpha:]]{3} [[:digit:]]{2})")
dat_POP4 <- as_tibble(x, .name_repair = "unique")
colnames(dat_POP4) <- "pressReleases"
dat_POP4$pressReleases <- str_replace_all(dat_POP4$pressReleases, "Jän ", "Jan ") # change Austrian "Jän" (i.e., "Jänner") to "Jan" (i.e., "January")
dat_POP4$pressReleases <- str_replace_all(dat_POP4$pressReleases, "Mär ", "Mar ") # change Austrian "Mär" (i.e., "März") to "Mar" (i.e., "March")
dat_POP4$pressReleases <- str_replace_all(dat_POP4$pressReleases, "Mai ", "May ") # change Austrian "Mai" (i.e., "Mai") to "May" (i.e., "May")
dat_POP4$pressReleases <- str_replace_all(dat_POP4$pressReleases, "Okt ", "Oct ") # change Austrian "Oktober" (i.e., "Okt") to "Oct" (i.e., "October")
dat_POP4$pressReleases <- str_replace_all(dat_POP4$pressReleases, "Dez ", "Dec ") # change Austrian "Dez" (i.e., "Dezember") to "Dec" (i.e., "December")

dat_POP4 <- dat_POP4 %>% 
  mutate(date = str_extract(dat_POP4$pressReleases, "[[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4}")) %>% 
  mutate(ots = str_extract(dat_POP4$pressReleases, "OTS[0-9]{4}")) %>% 
  mutate(text = str_extract(dat_POP4$pressReleases, "([[:digit:]]{2}.[[:alpha:]]{3} [[:digit:]]{4})((.|\n)*?)(~)")) %>% 
  mutate(cabinet = "Schallenberg/Nehammer") %>%  # Cabinets Schallenberg/Nehammer
  mutate(ministry = "FMEIA") %>% # Federal Ministry of European and International Affairs
  select(-pressReleases)

dat_POP4$date <- dmy(dat_POP4$date) # transform date to date format
dat_POP4$text <- gsub("\n", " ", dat_POP4$text) # remove /n from text


# combine three datasets to a single one

dat_POP <- bind_rows(dat_POP1, dat_POP2)
dat_POP <- bind_rows(dat_POP, dat_POP3)
dat_POP <- bind_rows(dat_POP, dat_POP4)
dat_POP$month <- format(as.Date(dat_POP$date), "%Y-%m") # add month variable
dat_POP$year <- format(as.Date(dat_POP$date), "%Y") # add year variable
dat_POP$Qyear <- as.yearqtr(dat_POP$date, format = "%Y-%m-%d") # add Quarters
dat_POP$Qyear <- as.character(dat_POP$Qyear)

# remove ots with "presseeinladung" and "reminder" in text

dat_POP <-  dat_POP[!grepl("Presseeinladung", dat_POP$text),]
dat_POP <-  dat_POP[!grepl("Reminder", dat_POP$text),]

# save final dataframe objects to file
save(dat_POP, file = "objects_fp-pop_010.RData")

quit()
