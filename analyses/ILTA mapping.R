## Prototype ILTA mapping script - reads source data from both Snowflake
## databases (AVA and CASEWORK) before transformation.

library(ascFuncs)
library(tidyverse)
library(logger)
library(feasts)
library(tsibble)

## --- Load AVA data -----------------------------------------------------------

con_ava <- ascFuncs::connect_snowflake(database = "AVA")
df_ava <- DBI::dbReadTable(con_ava, "ACCESSAVA")
DBI::dbDisconnect(con_ava)

## --- Load CASEWORK data ------------------------------------------------------

con_casework <- ascFuncs::connect_snowflake(database = "CASEWORK")
df_casework <- DBI::dbReadTable(con_casework, "ADVICEPRO_CASEWORK")
DBI::dbDisconnect(con_casework)


df_casework_demo <- query_advicepro_report("HS6T5CH9")


## --- Log row counts ----------------------------------------------------------

logger::log_info("ACCESSAVA rows loaded: {nrow(df_ava)}")
logger::log_info("ADVICEPRO_CASEWORK rows loaded: {nrow(df_casework)}")

# # ILTA mapping transformation (one time, exploratory)

# df_ava %>% as.tibble %>% 
#     ## only get demographic fields
#     select(disability_n_y, disability_text,  ethnicity, age_range ) %>%
#     map(unique)


# df_casework_demo %>% as.tibble %>%
#     ## only get demographic fields
#     map(unique)


## --- Read mapping tables from Snowflake -------------------------------------

con_ref <- connect_snowflake(database = "REFERENCE")
ilta_map_ava      <- DBI::dbReadTable(con_ref, "ILTA_MAP_AVA")
ilta_map_casework <- DBI::dbReadTable(con_ref, "ILTA_MAP_CASEWORK")
DBI::dbDisconnect(con_ref)

logger::log_info("ILTA mapping tables loaded from REFERENCE.PUBLIC")


ava_counts <- df_ava %>% 
  filter(created_at >= "2025-10-01", created_at < "2026-03-31") %>%
  select(transcript_id, disability_text, disability_n_y, age_range, ethnicity) %>% 
  pivot_longer(cols = c(disability_text, disability_n_y, age_range, ethnicity), names_to = "FEATURE", values_to = "ACCESSAVA") %>%
  # filter(!is.na(ACCESSAVA)) %>% 
  left_join(ilta_map_ava) %>% 
  count(FEATURE, ILTA)

casework_counts <- df_casework_demo %>%
  mutate(date = as.Date(`Case Open Date`, format = "%d/%m/%Y")) %>%
  filter(date >= "2025-10-01", date < "2026-03-31") %>%
  select(-date, -`Case Open Date`) %>% 
  uncount(`Number of Person with Care Needss`) %>% 
  pivot_longer(cols  = everything(), names_to = "FEATURE", values_to = "CASEWORK") %>% 
  mutate(CASEWORK = ifelse(CASEWORK == "[Not Specified]" | CASEWORK == "Not Stated", NA_character_, CASEWORK)) %>%
  # filter(!is.na(CASEWORK)) %>% 
  select(-FEATURE) %>% 
  left_join(ilta_map_casework)%>%
  mutate(ifelse(FEATURE == "Do you have a disability", "disability_n_y", FEATURE)) %>%
  count(FEATURE, ILTA)
  
total_counts <- ava_counts %>% 
  bind_rows(casework_counts) %>% 
  group_by(FEATURE, ILTA) %>% 
  summarise(TOTAL = sum(n), .groups = "drop")

total_counts %>% 
  print(n=Inf)

## --- Impact analysis ---------------------------------------------------------

## Pick whichever LA column AVA uses
la_col_ava <- if (any(names(df_ava) == "local_authority_name")) {
  "local_authority_name"
} else {
  "local_authority"
}

## Block 1: Combined long-format dataset
df_combined <- dplyr::bind_rows(
  df_ava %>%
    dplyr::transmute(
      source   = "AVA",
      id       = transcript_id,
      month    = format(as.Date(created_at), "%Y-%m"),
      category = supercategories,
      la       = la_name
    ),
  df_casework %>%
    dplyr::transmute(
      source   = "Casework",
      id       = case_reference,
      month    = format(
        as.Date(paste0(case_open_month, "/01"), format = "%Y/%m/%d"),
        "%Y-%m"
      ),
      category = super_category,
      la       = la_name
    )
) %>%
  tidyr::separate_rows(category, sep = ";") %>%
  dplyr::mutate(category = stringr::str_trim(category))

## Plot 1: Year-on-year seasonal plot (feasts::gg_season)
p1 <- df_combined %>%
  dplyr::count(source, month) %>%
  dplyr::mutate(month = tsibble::yearmonth(month)) %>%
  tsibble::as_tsibble(index = month, key = source) %>%
  tsibble::fill_gaps() %>% 
  ggtime::gg_season(n, labels = "right") +
  labs(title = "Interactions by month (year-on-year)", x = NULL, y = "Count") +
  facet_wrap(~source, ncol = 1, scales = "free_y") +
  theme_minimal()

print(p1)

## Plot 2: Total interactions by category and source (top 20)
top_cats <- df_combined %>%
  dplyr::filter(!is.na(category), category != "") %>%
  dplyr::count(category, sort = TRUE) %>%
  dplyr::slice_head(n = 20) %>%
  dplyr::pull(category)

p2 <- df_combined %>%
  dplyr::filter(category %in% top_cats) %>%
  dplyr::count(source, category) %>%
  dplyr::mutate(category = forcats::fct_reorder(category, n, sum)) %>%
  ggplot(aes(x = n, y = category, fill = source)) +
  geom_col(position = "stack") +
  labs(title = "Top 20 categories", x = "Count", y = NULL, fill = "Source") +
  theme_minimal()

print(p2)

## Plot 3: Total interactions by local authority (top 20)
top_las <- df_combined %>%
  dplyr::filter(!is.na(la)) %>%
  dplyr::count(la, sort = TRUE) %>%
  dplyr::slice_head(n = 20) %>%
  dplyr::pull(la)

p3 <- df_combined %>%
  dplyr::filter(la %in% top_las) %>%
  dplyr::count(source, la) %>%
  dplyr::mutate(la = forcats::fct_reorder(la, n, sum)) %>%
  ggplot(aes(x = n, y = la, fill = source)) +
  geom_col(position = "stack") +
  labs(title = "Top 20 local authorities", x = "Count", y = NULL, fill = "Source") +
  theme_minimal()

print(p3)

## Block 5: Overall totals
logger::log_info(
  "Total AVA conversations: {nrow(df_ava)}"
)
logger::log_info(
  "Total Casework cases: {nrow(df_casework)}"
)
logger::log_info(
  "Combined total interactions (after category explode): {nrow(df_combined)}"
)






new_ava_rows <- tibble::tibble(
  FEATURE   = "disability_n_y",
  ACCESSAVA = c("Y", "N"),
  ILTA      = c("Yes", "No")
)

new_casework_rows <- tibble::tibble(
  FEATURE   = "Do you have a disability",
  CASEWORK = c("Yes", "No"),
  ILTA      = c("Yes", "No")
)

con_ref <- ascFuncs::connect_snowflake(database = "REFERENCE")
DBI::dbWriteTable(con_ref, "ILTA_MAP_AVA", new_ava_rows, append = TRUE)
DBI::dbWriteTable(con_ref, "ILTA_MAP_CASEWORK", new_casework_rows, append = TRUE)

