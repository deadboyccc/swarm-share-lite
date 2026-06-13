# NEEDED BECAUSE GITHUB DOESN'T TRACK EMPTY FOLDER -

# Create the config folder and add .gitkeep
mkdir -p config && touch config/.gitkeep

# Create core module directories and add .gitkeep
mkdir -p core/src/main/java/io/swarmshare/core/domain && touch core/src/main/java/io/swarmshare/core/domain/.gitkeep
mkdir -p core/src/main/java/io/swarmshare/core/port && touch core/src/main/java/io/swarmshare/core/port/.gitkeep
mkdir -p core/src/test/java/io/swarmshare/core/domain && touch core/src/test/java/io/swarmshare/core/domain/.gitkeep

# Create manifest module directories and add .gitkeep
mkdir -p manifest/src/main/java/io/swarmshare/manifest && touch manifest/src/main/java/io/swarmshare/manifest/.gitkeep
mkdir -p manifest/src/test/java/io/swarmshare/manifest && touch manifest/src/test/java/io/swarmshare/manifest/.gitkeep

# Create storage module directories and add .gitkeep
mkdir -p storage/src/main/java/io/swarmshare/storage && touch storage/src/main/java/io/swarmshare/storage/.gitkeep
mkdir -p storage/src/test/java/io/swarmshare/storage && touch storage/src/test/java/io/swarmshare/storage/.gitkeep

# Create networking module directories and add .gitkeep
mkdir -p networking/src/main/java/io/swarmshare/networking && touch networking/src/main/java/io/swarmshare/networking/.gitkeep
mkdir -p networking/src/test/java/io/swarmshare/networking && touch networking/src/test/java/io/swarmshare/networking/.gitkeep

# Create transfer module directories and add .gitkeep
mkdir -p transfer/src/main/java/io/swarmshare/transfer && touch transfer/src/main/java/io/swarmshare/transfer/.gitkeep
mkdir -p transfer/src/test/java/io/swarmshare/transfer && touch transfer/src/test/java/io/swarmshare/transfer/.gitkeep

# Create cli module directories and add .gitkeep
mkdir -p cli/src/main/java/io/swarmshare/cli && touch cli/src/main/java/io/swarmshare/cli/.gitkeep
