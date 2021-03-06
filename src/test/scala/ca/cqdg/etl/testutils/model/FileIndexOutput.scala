/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-05-25T15:29:23.354188
 */
package ca.cqdg.etl.testutils.model


case class FileIndexOutput(`file_name_keyword`: String = "zplTjYga3.pdf",
                           `file_name_ngrams`: String = "zplTjYga3.pdf",
                           `study_id`: String = "ST0003",
                           `file_name`: String = "zplTjYga3.pdf",
                           `data_category`: String = "Clinical",
                           `data_type`: String = "Pathological report",
                           `is_harmonized`: Boolean = false,
                           `experimental_strategy`: Option[String] = None,
                           `data_access`: String = "Controled",
                           `file_format`: String = "pdf",
                           `platform`: Option[String] = None,
                           `file_size`: Double = 24.513199347151154,
                           `study`: List[STUDY] = List(STUDY(
                             `description` = "population-based cohort focusing on complex conditions",
                             `domain` = "General Health",
                             `keyword` = "Common chronic disorders; Prospective cohort; Reference genome",
                             `name` = "Study1",
                             `population` = "Adult",
                             `short_name` = "ST1",
                             `short_name_keyword` = "ST1",
                             `short_name_ngrams` = "ST1",
                             `study_id` = "ST0001",
                             `study_id_keyword` = "ST0001")),
                           `file_variant_class`: String = "no-data",
                           `donors`: List[DONORS] = List(DONORS()),
                           `biospecimen`: List[BIOSPECIMEN] = List(BIOSPECIMEN()),
                           `diagnoses`: List[DIAGNOSES] = List(DIAGNOSES()),
                           `phenotypes`: List[ONTOLOGY_TERM] = List(ONTOLOGY_TERM()),
                           `dictionary_version`: String = "5.44",
                           `study_version`: String = "1.0",
                           `study_version_creation_date`: String = "2020/05/01")

case class DONORS(`age_at_recruitment`: String = "57",
                  `date_of_recruitment`: String = "5/10/2009",
                  `diagnoses`: List[DIAGNOSES] = List(DIAGNOSES()),
                  `dob`: String = "2/21/1951",
                  `environment_exposure_available`: String = "NO",
                  `ethnicity`: String = "French Canadian ",
                  `exposure`: List[FILE_EXPOSURE_EXPOSURE] = List(FILE_EXPOSURE_EXPOSURE()),
                  `exposures`: List[FILE_EXPOSURE_EXPOSURE] = List(FILE_EXPOSURE_EXPOSURE()),
                  `familyConditions`: List[FAMILYCONDITIONS] = List(FAMILYCONDITIONS()),
                  `familyHistory`: List[FAMILYHISTORY] = List(FAMILYHISTORY(
                    `family_cancer_history` = "Yes",
                    `family_condition_age` = 48,
                    `family_condition_name` = "Multiple sclerosis",
                    `family_condition_relationship` = "maternal aunt",
                    `submitter_family_condition_id` = "FC00060")),
                  `family_history_available`: String = "NO",
                  `files`: List[FILES] = List(FILES()),
                  `gender`: String = "Female",
                  `genealogy_available`: String = "YES",
                  `is_a_proband`: String = "Not applicable",
                  `is_affected`: String = "Not applicable",
                  `laboratory_measures_available`: String = "NO",
                  `lifestyle_available`: String = "NO",
                  `medication_available`: String = "YES",
                  `phenotypes`: List[ONTOLOGY_TERM] = List(ONTOLOGY_TERM()),
                  `physical_measures_available`: String = "NO",
                  `study`: List[STUDY] = List(STUDY()),
                  `submitter_donor_id`: String = "PT00060",
                  `vital_status`: String = "alive")

case class DIAGNOSES(`age_at_diagnosis`: Long = 52,
                     `diagnosis_type`: String = "Clinical",
                     `follow_ups`: List[FOLLOW_UPS] = List(FOLLOW_UPS()),
                     `is_cancer`: Boolean = true,
                     `is_cancer_primary`: String = "Yes",
                     `is_self_reported`: String = "Yes",
                     `m_category`: String = "M1d(0)",
                     `n_category`: String = "N2",
                     `stage_group`: String = "stage iia",
                     `submitter_diagnosis_id`: String = "DI00060",
                     `t_category`: String = "Tis(LCIS)",
                     `treatments`: List[TREATMENTS] = List(TREATMENTS()))

case class FILE_EXPOSURE_EXPOSURE(`FSA`: String = "H4V",
                                  `alcohol_status`: String = "Alcohol consumption unknown",
                                  `smoking_status`: String = "Current every day smoker        ")
