import unittest
from csv_loader import CsvLoader


class TestCsvLoader(unittest.TestCase):

    csv_sample_filename = "resources/stackoverflow_survey_results_public_sample.csv"

    def test_original_headers(self):
        loader = CsvLoader(self.csv_sample_filename)

        # print(loader.original_headers)
        # print(loader.headers)

        self.assertEqual(loader.original_headers, self.original_headers)

    def test_headers(self):
        loader = CsvLoader(self.csv_sample_filename)

        # print(loader.original_headers)
        # print(loader.headers)

        self.assertEqual(loader.headers, self.unified_headers)

    original_headers = ['Respondent', 'Professional', 'ProgramHobby', 'Country', 'University', 'EmploymentStatus',
                        'FormalEducation', 'MajorUndergrad', 'HomeRemote', 'CompanySize', 'CompanyType', 'YearsProgram',
                        'YearsCodedJob', 'YearsCodedJobPast', 'DeveloperType', 'WebDeveloperType',
                        'MobileDeveloperType', 'NonDeveloperType', 'CareerSatisfaction', 'JobSatisfaction',
                        'ExCoderReturn', 'ExCoderNotForMe', 'ExCoderBalance', 'ExCoder10Years', 'ExCoderBelonged',
                        'ExCoderSkills', 'ExCoderWillNotCode', 'ExCoderActive', 'PronounceGIF', 'ProblemSolving',
                        'BuildingThings', 'LearningNewTech', 'BoringDetails', 'JobSecurity', 'DiversityImportant',
                        'AnnoyingUI', 'FriendsDevelopers', 'RightWrongWay', 'UnderstandComputers', 'SeriousWork',
                        'InvestTimeTools', 'WorkPayCare', 'KinshipDevelopers', 'ChallengeMyself', 'CompetePeers',
                        'ChangeWorld', 'JobSeekingStatus', 'HoursPerWeek', 'LastNewJob', 'AssessJobIndustry',
                        'AssessJobRole', 'AssessJobExp', 'AssessJobDept', 'AssessJobTech', 'AssessJobProjects',
                        'AssessJobCompensation', 'AssessJobOffice', 'AssessJobCommute', 'AssessJobRemote',
                        'AssessJobLeaders', 'AssessJobProfDevel', 'AssessJobDiversity', 'AssessJobProduct',
                        'AssessJobFinances', 'ImportantBenefits', 'ClickyKeys', 'JobProfile', 'ResumePrompted',
                        'LearnedHiring', 'ImportantHiringAlgorithms', 'ImportantHiringTechExp',
                        'ImportantHiringCommunication', 'ImportantHiringOpenSource', 'ImportantHiringPMExp',
                        'ImportantHiringCompanies', 'ImportantHiringTitles', 'ImportantHiringEducation',
                        'ImportantHiringRep', 'ImportantHiringGettingThingsDone', 'Currency', 'Overpaid', 'TabsSpaces',
                        'EducationImportant', 'EducationTypes', 'SelfTaughtTypes', 'TimeAfterBootcamp',
                        'CousinEducation', 'WorkStart', 'HaveWorkedLanguage', 'WantWorkLanguage', 'HaveWorkedFramework',
                        'WantWorkFramework', 'HaveWorkedDatabase', 'WantWorkDatabase', 'HaveWorkedPlatform',
                        'WantWorkPlatform', 'IDE', 'AuditoryEnvironment', 'Methodology', 'VersionControl',
                        'CheckInCode', 'ShipIt', 'OtherPeoplesCode', 'ProjectManagement', 'EnjoyDebugging', 'InTheZone',
                        'DifficultCommunication', 'CollaborateRemote', 'MetricAssess', 'EquipmentSatisfiedMonitors',
                        'EquipmentSatisfiedCPU', 'EquipmentSatisfiedRAM', 'EquipmentSatisfiedStorage',
                        'EquipmentSatisfiedRW', 'InfluenceInternet', 'InfluenceWorkstation', 'InfluenceHardware',
                        'InfluenceServers', 'InfluenceTechStack', 'InfluenceDeptTech', 'InfluenceVizTools',
                        'InfluenceDatabase', 'InfluenceCloud', 'InfluenceConsultants', 'InfluenceRecruitment',
                        'InfluenceCommunication', 'StackOverflowDescribes', 'StackOverflowSatisfaction',
                        'StackOverflowDevices', 'StackOverflowFoundAnswer', 'StackOverflowCopiedCode',
                        'StackOverflowJobListing', 'StackOverflowCompanyPage', 'StackOverflowJobSearch',
                        'StackOverflowNewQuestion', 'StackOverflowAnswer', 'StackOverflowMetaChat',
                        'StackOverflowAdsRelevant', 'StackOverflowAdsDistracting', 'StackOverflowModeration',
                        'StackOverflowCommunity', 'StackOverflowHelpful', 'StackOverflowBetter', 'StackOverflowWhatDo',
                        'StackOverflowMakeMoney', 'Gender', 'HighestEducationParents', 'Race', 'SurveyLong',
                        'QuestionsInteresting', 'QuestionsConfusing', 'InterestedAnswers', 'Salary', 'ExpectedSalary']

    unified_headers = ['respondent', 'professional', 'program_hobby', 'country', 'university', 'employment_status',
                       'formal_education', 'major_undergrad', 'home_remote', 'company_size', 'company_type',
                       'years_program', 'years_coded_job', 'years_coded_job_past', 'developer_type',
                       'web_developer_type', 'mobile_developer_type', 'non_developer_type', 'career_satisfaction',
                       'job_satisfaction', 'ex_coder_return', 'ex_coder_not_for_me', 'ex_coder_balance',
                       'ex_coder10_years', 'ex_coder_belonged', 'ex_coder_skills', 'ex_coder_will_not_code',
                       'ex_coder_active', 'pronounce_g_i_f', 'problem_solving', 'building_things', 'learning_new_tech',
                       'boring_details', 'job_security', 'diversity_important', 'annoying_u_i', 'friends_developers',
                       'right_wrong_way', 'understand_computers', 'serious_work', 'invest_time_tools', 'work_pay_care',
                       'kinship_developers', 'challenge_myself', 'compete_peers', 'change_world', 'job_seeking_status',
                       'hours_per_week', 'last_new_job', 'assess_job_industry', 'assess_job_role', 'assess_job_exp',
                       'assess_job_dept', 'assess_job_tech', 'assess_job_projects', 'assess_job_compensation',
                       'assess_job_office', 'assess_job_commute', 'assess_job_remote', 'assess_job_leaders',
                       'assess_job_prof_devel', 'assess_job_diversity', 'assess_job_product', 'assess_job_finances',
                       'important_benefits', 'clicky_keys', 'job_profile', 'resume_prompted', 'learned_hiring',
                       'important_hiring_algorithms', 'important_hiring_tech_exp', 'important_hiring_communication',
                       'important_hiring_open_source', 'important_hiring_p_m_exp', 'important_hiring_companies',
                       'important_hiring_titles', 'important_hiring_education', 'important_hiring_rep',
                       'important_hiring_getting_things_done', 'currency', 'overpaid', 'tabs_spaces',
                       'education_important', 'education_types', 'self_taught_types', 'time_after_bootcamp',
                       'cousin_education', 'work_start', 'have_worked_language', 'want_work_language',
                       'have_worked_framework', 'want_work_framework', 'have_worked_database', 'want_work_database',
                       'have_worked_platform', 'want_work_platform', 'i_d_e', 'auditory_environment', 'methodology',
                       'version_control', 'check_in_code', 'ship_it', 'other_peoples_code', 'project_management',
                       'enjoy_debugging', 'in_the_zone', 'difficult_communication', 'collaborate_remote',
                       'metric_assess', 'equipment_satisfied_monitors', 'equipment_satisfied_c_p_u',
                       'equipment_satisfied_r_a_m', 'equipment_satisfied_storage', 'equipment_satisfied_r_w',
                       'influence_internet', 'influence_workstation', 'influence_hardware', 'influence_servers',
                       'influence_tech_stack', 'influence_dept_tech', 'influence_viz_tools', 'influence_database',
                       'influence_cloud', 'influence_consultants', 'influence_recruitment', 'influence_communication',
                       'stack_overflow_describes', 'stack_overflow_satisfaction', 'stack_overflow_devices',
                       'stack_overflow_found_answer', 'stack_overflow_copied_code', 'stack_overflow_job_listing',
                       'stack_overflow_company_page', 'stack_overflow_job_search', 'stack_overflow_new_question',
                       'stack_overflow_answer', 'stack_overflow_meta_chat', 'stack_overflow_ads_relevant',
                       'stack_overflow_ads_distracting', 'stack_overflow_moderation', 'stack_overflow_community',
                       'stack_overflow_helpful', 'stack_overflow_better', 'stack_overflow_what_do',
                       'stack_overflow_make_money', 'gender', 'highest_education_parents', 'race', 'survey_long',
                       'questions_interesting', 'questions_confusing', 'interested_answers', 'salary',
                       'expected_salary']


if __name__ == '__main__':
    unittest.main()
