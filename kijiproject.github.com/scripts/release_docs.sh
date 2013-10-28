#!/usr/bin/env bash
#
# Kiji documentation release script
# docs.kiji.org
#
# This script creates new copies of userguides and tutorials based on the development
# versions being worked on in directories named **/DEVEL.
#
# If executed as-is, nothing will change. You must update the environment variables
# at the top of this script to publish new documentation versions that correspond
# with concurrent Kiji module releases. New documentation (e.g., for the music
# recommendation tutorial) may refer to previous versions of dependencies like
# KijiSchema. If KijiSchema is not being released but the Music Recommendation
# tutorial is, you should leave the SCHEMA_VER variable alone.
#
# This script also updates _config.yml with newly-required handles to documentation
# versions.
#
# You must create links from userguides.md and apidocs/index.md to new reified
# documentation yourself.
#
# Please commit the changes to the script when you run it to generate new reified
# documentation instances from the DEVEL directories.

# Version numbers must be given in two forms:
#  * The FLAT_VERSION_NUMBER (e.g., 1_0_0_rc2)
#    used in the _config.yml file to refer to versions of modules
#
#  * The REGULAR_VER (e.g., 1.0.0-rc2)
#    that corresponds to the true version number as recorded by Maven to git
#    and our release artifacts.


# KijiSchema version
SCHEMA_FLAT_VER=1_3_2
SCHEMA_VER=1.3.2

# KijiMR version
KIJIMR_FLAT_VER=1_2_0
KIJIMR_VER=1.2.0

# KijiMR Library version
MRLIB_FLAT_VER=1_1_0
MRLIB_VER=1.1.0

# KijiExpress version
EXPRESS_FLAT_VER=0_10_0
EXPRESS_VER=0.10.0

# Scoring version
SCORING_FLAT_VER=0_7_0
SCORING_VER=0.7.0

# Music Recommendation Tutorial version
MUSIC_FLAT_VER=1_1_0
MUSIC_VER=1.1.0

# Express Music Recommendation tutorial version
EXPRESS_MUSIC_FLAT_VER=0_9_0
EXPRESS_MUSIC_VER=0.9.0

# Phonebook Tutorial version
PHONEBOOK_FLAT_VER=1_1_0
PHONEBOOK_VER=1.1.0

# REST Server version
REST_FLAT_VER=0_6_0
REST_VER=0.6.0

### Ordinary configuration does not go past this line ###

# Constants used later in this script
API=http://api-docs.kiji.org

bin=`dirname $0`
bin=`cd "$bin" && pwd`
top=`cd "$bin/.." && pwd`

set -e

# Change the DEVEL macros that point to the latest version under development
# to the true version numbers that are consistent at the time of release.
# For instance, KijiMR may refer to the KijiSchema userguide; this will not
# point to userguide_schema_devel, but userguide_schema_$SCHEMA_FLAT_VER instead.
#
# This same process is applied to all newly-released documentation artifacts.
fix_released_versions() {
  # This function operates on all markdown files under the current directory, recursively.
  # It is expected to be called within a specific directory to reify.

  # Reify references to module documentation.
  find . -name "*.md" -exec sed -i -e "s/api_mrlib_devel/api_mrlib_$MRLIB_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_mr_devel/api_mr_$KIJIMR_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_schema_devel/api_schema_$SCHEMA_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_express_devel/api_express_$EXPRESS_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_mapreduce_devel/userguide_mapreduce_$KIJIMR_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_schema_devel/userguide_schema_$SCHEMA_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_rest_devel/userguide_rest_$REST_FLAT_VER/g" {} \;

  find . -name "*.md" -exec sed -i -e \
      "s/tutorial_phonebook_devel/tutorial_phonebook_$PHONEBOOK_FLAT_VER/g" {} \;

  # Reify git tags that turn into code snippits and accordions.
  find . -name "*.md" -exec sed -i -e \
      's/{{site.schema_devel_branch}}/'"kiji-schema-root-$SCHEMA_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mr_devel_branch}}/'"kiji-mapreduce-root-$KIJIMR_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mrlib_devel_branch}}/'"kiji-mapreduce-lib-root-$MRLIB_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_devel_branch}}/'"kiji-music-$MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_express_devel_branch}}/'"kiji-express-music-$EXPRESS_MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.rest_devel_branch}}/'"kiji-rest-root-$REST_VER/g" {} \;

  # Update HTML links to tutorial elements
  find . -name "*.md" -exec sed -i -e \
      "s|schema/DEVEL|schema/$SCHEMA_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|mapreduce/DEVEL|mapreduce/$KIJIMR_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|phonebook/DEVEL|phonebook/$PHONEBOOK_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|music-recommendation/DEVEL|music-recommendation/$MUSIC_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|express-recommendation/DEVEL|express-recommendation/$EXPRESS_MUSIC_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|rest/DEVEL|rest/$REST_VER|g" {} \;

  # Reify release version numbers in the text.
  find . -name "*.md" -exec sed -i -e \
      's/{{site.schema_devel_version}}/'"$SCHEMA_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mr_devel_version}}/'"$KIJIMR_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mrlib_devel_version}}/'"$MRLIB_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.phonebook_devel_version}}/'"$PHONEBOOK_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_devel_version}}/'"$MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_express_devel_version}}/'"$EXPRESS_MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.express_devel_version}}/'"$EXPRESS_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.rest_devel_version}}/'"$REST_VER/g" {} \;
}


# In turn, release each individual documentation component.
cd "$top/_posts"

if [ ! -d "userguides/schema/$SCHEMA_VER" ]; then
  # Create new KijiSchema documentation
  echo "Creating new KijiSchema user guide: $SCHEMA_VER"
  cp -ra "userguides/schema/DEVEL" "userguides/schema/$SCHEMA_VER"

  pushd "userguides/schema/$SCHEMA_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e "s/version: devel/version: $SCHEMA_VER/" {} \;
  find . -name "*.md" -exec sed -i -e "s/schema, devel]/schema, $SCHEMA_VER]/" {} \;

  # Replace links to development userguides and API documentation with the real latest
  # documentation artifact version macros (defined in /_config.yml).
  fix_released_versions

  # Define the new KijiSchema release in /_config.yml
  echo "userguide_schema_$SCHEMA_FLAT_VER : /userguides/schema/$SCHEMA_VER" \
      >> "$top/_config.yml"
  echo "api_schema_$SCHEMA_FLAT_VER : $API/kiji-schema/$SCHEMA_VER/org/kiji/schema" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/mapreduce/$KIJIMR_VER" ]; then
  # Create new KijiMR documentation
  echo "Creating new KijiMR user guide: $KIJIMR_VER"
  cp -ra "userguides/mapreduce/DEVEL" "userguides/mapreduce/$KIJIMR_VER"

  pushd "userguides/mapreduce/$KIJIMR_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e \
      "s/version: devel/version: $KIJIMR_VER/" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/mapreduce, devel]/mapreduce, $KIJIMR_VER]/" {} \;

  fix_released_versions

  echo "api_mr_$KIJIMR_FLAT_VER : $API/kiji-mapreduce/$KIJIMR_VER/org/kiji/mapreduce" \
      >> "$top/_config.yml"
  echo "userguide_mapreduce_$KIJIMR_FLAT_VER : /userguides/mapreduce/$KIJIMR_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/rest/$REST_VER" ]; then
  # Create new REST documentation
  echo "Creating new KijiREST user guide: $REST_VER"
  cp -ra "userguides/rest/DEVEL" "userguides/rest/$REST_VER"

  pushd "userguides/rest/$REST_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e \
      "s/version: devel/version: $REST_VER/" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/rest, devel]/rest, $REST_VER]/" {} \;

  fix_released_versions

  echo "userguide_rest_$REST_FLAT_VER : /userguides/rest/$REST_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/phonebook/$PHONEBOOK_VER" ]; then
  # Create a new phonebook tutorial
  echo "Creating new Phonebook tutorial: $PHONEBOOK_VER"
  cp -ra "tutorials/phonebook/DEVEL" "tutorials/phonebook/$PHONEBOOK_VER"

  pushd "tutorials/phonebook/$PHONEBOOK_VER"

  find . -name "*.md" -exec sed -i -e \
      "s/phonebook-tutorial, devel]/phonebook-tutorial, $PHONEBOOK_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_phonebook_$PHONEBOOK_FLAT_VER : /tutorials/phonebook-tutorial/$PHONEBOOK_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/music-recommendation/$MUSIC_VER" ]; then
  echo "Creating a new Music recommendation tutorial: $MUSIC_VER"
  cp -ra "tutorials/music-recommendation/DEVEL" "tutorials/music-recommendation/$MUSIC_VER"

  pushd "tutorials/music-recommendation/$MUSIC_VER"

  # Reify this version number
  find . -name "*.md" -exec sed -i -e \
      "s/music-recommendation, devel]/music-recommendation, $MUSIC_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_music_$MUSIC_FLAT_VER : /tutorials/music-recommendation/$MUSIC_VER" \
      >> _config.yml

  popd
fi

if [ ! -d "tutorials/express-recommendation/$EXPRESS_MUSIC_VER" ]; then
  echo "Creating a new Express music recommendation tutorial: $EXPRESS_MUSIC_VER"
  cp -ra "tutorials/express-recommendation/DEVEL" \
      "tutorials/express-recommendation/$EXPRESS_MUSIC_VER"

  pushd "tutorials/express-recommendation/$EXPRESS_MUSIC_VER"

  # Reify this version number
  find . -name "*.md" -exec sed -i -e \
      "s/express-recommendation, devel]/express-recommendation, $EXPRESS_MUSIC_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_exp_music_$EXPRESS_MUSIC_FLAT_VER : /tutorials/express-recommendation/$EXPRESS_MUSIC_VER" \
      >> _config.yml

  popd
fi

# Check: If a new version of KijiMR lib is available than previously declared in
# _config.yml, add the api_mrlib_$MRLIB_FLAT_VER reference to the _config.yml.
set +e # It's ok to get a non-zero return value here.
grep "api_mrlib_$MRLIB_FLAT_VER :" "$top/_config.yml" >/dev/null
if [ "$?" != "0" ]; then
  # We didn't find the API reference. Add KijiMR Library API docs reference to _config.yml.
  echo "Adding KijiMR Library API docs to _config.yml: $MRLIB_VER"
  echo "api_mrlib_$MRLIB_FLAT_VER : $API/kiji-mapreduce-lib/$MRLIB_VER/org/kiji/mapreduce/lib" \
      >> "$top/_config.yml"
fi

grep "api_express_$EXPRESS_FLAT_VER :" "$top/_config.yml" >/dev/null
if [ "$?" != "0" ]; then
  # We didn't find the API reference. Add Kiji Express API docs reference to _config.yml.
  echo "Adding Kiji Express API docs to _config.yml: $EXPRESS_VER"
  echo "api_express_$EXPRESS_FLAT_VER : $API/kiji-express/$EXPRESS_VER/org/kiji/express" \
      >> "$top/_config.yml"
fi

echo ""
echo "Automated documentation release steps complete."
echo ""
echo "There's still some manual work to be done - docs release is not complete yet!"
echo ""
echo "At this point you should:"
echo " * Create new links in userguides.md, apidocs/index.md, and tutorials.md that"
echo "   point to the newly released modules."
echo " * Update the devel macros in /_config.yml to point to the next versions."
echo " * Commit these changes and push to master."
echo ""

