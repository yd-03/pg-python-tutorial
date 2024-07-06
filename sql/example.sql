CREATE TABLE happiness (
    survey_yr INTEGER NOT NULL,
    country VARCHAR(24) NOT NULL,
    overall_rank INTEGER NOT NULL,
    score DECIMAL(15, 14) NOT NULL,
    gdp DECIMAL(17, 16) NOT NULL,
    generosity DECIMAL(16, 16) NOT NULL,
    freedom DECIMAL(16, 16) NOT NULL,
    social_support DECIMAL(16, 15) NOT NULL,
    life_exp DECIMAL(17, 16) NOT NULL,
    gov_trust DECIMAL(16, 16) NOT NULL
);

INSERT INTO happiness (survey_yr, country, overall_rank, score, gdp, generosity, freedom, social_support, life_exp, gov_trust) VALUES (19, 'Finland', 1, 7.769, 1.340, 0.153, 0.596, 1.573, 0.986, 0.393);
INSERT INTO happiness (survey_yr, country, overall_rank, score, gdp, generosity, freedom, social_support, life_exp, gov_trust) VALUES (19, 'Denmark', 2, 7.600, 1.383, 0.252, 0.592, 1.582, 0.996, 0.410);