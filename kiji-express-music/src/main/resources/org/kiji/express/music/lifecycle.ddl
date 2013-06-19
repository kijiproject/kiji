EXTRACT SCORE
  USING MODEL DEFINITION INFILE 'modeling/recommender-model-definition.json'
  USING MODEL ENVIRONMENT INFILE 'modeling/recommender-model-environment.json'
  LIBJARS ( 'lib/${project.build.finalName}.jar' );

