# Get a sample of companies that are required to report their energy usage to officials (AFiD)

# The following PLANTS are required to do so:
# all Bundesländer except Bayern (geht nur nicht im FDZ aber per Fernverarbeitung)
# firms in WZ_08 groups B (05.10 - 09.90) - Bergbau und die Gewinnung von Steinen und Erden
#                       D (10.11 - 33.20) - Verarbeitende Gewerbe
#   with at least 20 employees (at plant level)

if(!require(pacman)) install.packages(pacman)
pacman::p_load(tidyverse, readxl, haven, here)

# Function to create factors from stata variable
stata2factor <- function(x){
  map_tab <- stack(attr(x, 'labels'))
  
  if(length(map_tab$ind) == length(unique(x))){
    fac <- factor(x, labels = map_tab$ind)
  }
  
  else fac <- factor(x)
  
  return(fac)
}

# Read latest panel wave --------------------------------------------------

df_mup20 <- read_delim("I:\\!Projekte\\BMBF-2021-DynTOBI\\Daten\\mup\\final\\mup_2020.txt", delim = '\t', 
                       #n_max = 1000000,
                       col_types = cols(
                         crefo = col_character(),
                         year = col_double(),
                         entry = col_double(),
                         exit = col_double(),
                         name = col_character(),
                         address = col_character(),
                         age = col_double(),
                         sector = col_double(),
                         employees = col_double(),
                         turnover = col_double()
                       ))



plz2land <- read_excel("Q:\\Meine Bibliotheken\\Research\\04_Data\\02_MUP\\plz2land.xlsx")

df_stammw59 <- read_dta("K:\\MUP\\Paneldaten\\Stammdaten\\Aufbereitet\\Stammdaten_w59.dta")
map_refo <- stack(attr(df_stammw59$refo, 'labels')) %>% 
  mutate(values = as.factor(values)) %>% 
  rename(refo = values, legal_form = ind)

df_hrw58 <- read_dta("K:\\MUP\\Paneldaten\\HR_Umstr_Nr\\hr_nummer.dta")
df_ustw58 <- read_dta("K:\\MUP\\Paneldaten\\HR_Umstr_Nr\\umstnr_w58.dta")
# Filter AFiD firms -------------------------------------------------------


df_afid <- df_mup20 %>% 
  # extract plz info from address column
  mutate(plz = as.double(str_extract(string = address, pattern = regex(pattern = "\\d{5}")))) %>% 
  # merge land given plz using plz2land file
  left_join(plz2land %>% select(PLZ, Bundesland) %>% rename(land = Bundesland), by = c("plz" = "PLZ")) %>% 
  # apply filter criteria
  filter(land != "Bayern") %>% 
  filter(sector %in% c(5100:9900, 10110:33200)) %>% 
  filter(employees>=10) # this is strict as it also drops firms with no information on employee number


# Add legal form info
df_afid <- df_afid %>% 
  left_join(df_stammw59 %>% select(crefo, refo) %>% mutate(crefo = as.character(crefo)), by = "crefo") %>% 
  mutate(refo=stata2factor(refo)) %>% 
  left_join(map_refo, by = "refo") 
  

# Add HR number and Umsatzsteuer number
df_afid <- df_afid %>% 
  left_join(df_hrw58 %>% mutate(crefo = as.character(crefo)), by = "crefo") %>% 
  left_join(df_ustw58 %>% mutate(crefo = as.character(crefo)), by = "crefo") 
  
df_afid <- df_afid %>% 
  rename(hr_id = handelsregnummer, ust_id = umsatzsteuernummer) %>% 
  select(crefo, hr_id, ust_id, everything())

# Save data
df_afid %>% write_delim(path = "Q:\\Meine Bibliotheken\\Research\\01_Promotion\\05_Ideas\\06_GreenFinance\\05_Data\\mup2afid.txt", delim="\t")


# Get URL panel -----------------------------------------------------------

df_url <- read_delim("I:\\!Projekte\\BMBF-2021-DynTOBI\\Daten\\url\\raw\\web_adressen_panel.txt", delim="\t")
dim(df_url)

df_url <- df_url %>% 
  filter(crefo %in% as_vector(df_afid$crefo))
dim(df_url)

# Save data
df_url %>% write_delim(path = "Q:\\Meine Bibliotheken\\Research\\01_Promotion\\05_Ideas\\06_GreenFinance\\05_Data\\mup2afid_urls.txt", delim="\t")


# Get sample of AFiD firm URLs --------------------------------------------

df_afid_sample <- df_afid %>% 
  mutate(sector_group = ifelse(nchar(sector)==4, "C", "D")) %>% 
  mutate(size_group = factor(case_when(
    (employees <= 10) | (turnover <= 2000000) ~ 'Micro-enterprise',
    ((employees > 10) & (employees < 50)) | ((turnover > 2000000) & (turnover <= 10000000)) ~ 'Small enterprise', 
    ((employees >= 50) & (employees < 250)) | ((turnover > 10000000) & (turnover <= 50000000)) ~ 'Medium-sized enterprise', 
    (employees >= 250) | (turnover > 50000000) ~ 'Large enterprise'
  ), levels = c('Large enterprise', 'Medium-sized enterprise', 'Small enterprise', 'Micro-enterprise'))) %>% 
  mutate(legal_group = factor(case_when(
    refo %in% c(1, 2, 3, 4, 5, 6, 7, 8) ~ 'full liability',
    refo %in% c(9, 10, 11, 12, 13, 82) ~ 'limited liability',
  ), levels = c('limited liability', 'full liability'))) %>%
  group_by(land, sector_group, size_group, legal_group) %>% 
  sample_n(1) %>% 
  ungroup()

df_url_sample <- df_url %>% 
  filter(crefo %in% as_vector(df_afid_sample$crefo))
dim(df_url_sample)

# Save data
df_url_sample %>% write_delim(path = "Q:\\Meine Bibliotheken\\Research\\01_Promotion\\05_Ideas\\06_GreenFinance\\05_Data\\mup2afid_urls_sample.txt", delim="\t")
