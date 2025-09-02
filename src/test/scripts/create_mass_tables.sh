#!/bin/bash
OS_TYPE=""

# Function to display usage
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -u, --user <username>        MySQL username (default: root)"
    echo "  -p, --password <password>    MySQL password"
    echo "  -h, --host <host>            MySQL host (default: 127.0.0.1)"
    echo "  -P, --port <port>            MySQL port (default: 3306)"
    echo "  -d, --database <dbname>      Database name"
    echo "  -i, --instances <number>     Number of parallel instances"
    echo "  -t, --tables <number>        Tables per instance"
    echo "  -r, --records <number>       Records per table"
    echo "  -b, --batch <size>           Batch size for SQL execution"
    echo "  --recreate-db                Drop and recreate database if exists (default: false)"
    echo "  --recreate-tables            Drop and recreate tables if exist (default: false)"
    echo "  -c, --config <file>          Config file (optional)"
    echo "  --help                       Display this help message"
}

# Default values
DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER="root"
CONFIG_FILE="db_config.conf"
BATCH_SIZE=1000
RECREATE_DATABASE=false

# Function to get current timestamp in milliseconds (cross-platform compatible)
get_time_ms() {
    case "$OS_TYPE" in
        macos)
            echo $(($(date +%s) * 1000 + $(date +%N | cut -c1-3)))
            ;;
        linux)
            echo $(($(date +%s%N)/1000000))
            ;;
    esac
}

# Function to check environment and dependencies
check_environment() {
    echo "Checking environment and dependencies..."
    
    # Check OS type
    case "$(uname -s)" in
        Darwin*)
            echo "- Detected OS: macOS"
            OS_TYPE="macos"
            
            # Check if brew is installed
            if ! command -v brew >/dev/null 2>&1; then
                echo "Error: Homebrew is not installed. Please install it first:"
                echo "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                exit 1
            fi
            
            # Check if flock is installed
            if ! command -v flock >/dev/null 2>&1; then
                echo "Error: flock is not installed. Please install it with:"
                echo "  brew install flock"
                exit 1
            fi
            
            # Check if mysql client is installed
            if ! command -v mysql >/dev/null 2>&1; then
                echo "Error: MySQL client is not installed. Please install it with:"
                echo "  brew install mysql-client@8.0"
                echo "Then add it to your PATH:"
                echo "  echo 'export PATH=\"/opt/homebrew/opt/mysql-client@8.0/bin:\$PATH\"' >> ~/.zshrc"
                echo "  source ~/.zshrc"
                exit 1
            fi
            ;;
            
        Linux*)
            echo "- Detected OS: Linux"
            OS_TYPE="linux"
            
            # Check if mysql client is installed
            if ! command -v mysql >/dev/null 2>&1; then
                echo "Error: MySQL client is not installed. Please install it with:"
                echo "  sudo apt-get install mysql-client   # For Ubuntu/Debian"
                echo "  sudo yum install mysql-client       # For RHEL/CentOS"
                exit 1
            fi
            
            # flock is usually pre-installed on Linux, but check anyway
            if ! command -v flock >/dev/null 2>&1; then
                echo "Error: flock is not installed. Please install it with:"
                echo "  sudo apt-get install util-linux     # For Ubuntu/Debian"
                echo "  sudo yum install util-linux-ng      # For RHEL/CentOS"
                exit 1
            fi
            ;;
            
        *)
            echo "Error: Unsupported operating system"
            exit 1
            ;;
    esac
    
    # Check if required commands exist
    local required_commands=("tput" "awk" "bc" "date" "tr")
    local missing_commands=()
    
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -ne 0 ]; then
        echo "Error: Missing required commands: ${missing_commands[*]}"
        echo "Please install the necessary packages containing these commands."
        exit 1
    fi
    
    echo "✓ All required dependencies are installed"
    echo
}

# Function to generate random string (macOS and Linux compatible)
generate_random_string() {
    local length=$1
    LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c $length || true
}

# Function to create progress bar
create_progress_bar() {
    local percent=$1
    local width=30
    local completed=$((width * percent / 100))
    local remainder=$((width - completed))
    
    printf "["
    if [ $completed -gt 0 ]; then
        printf "%${completed}s" | tr ' ' '#'
    fi
    if [ $remainder -gt 0 ]; then
        printf "%${remainder}s" | tr ' ' '-'
    fi
    printf "]"
}

# First, add this helper function for generating insert data
generate_insert_data() {
    local table_name=$1
    local num_records=$2
    local data=""
    
    # Build INSERT statement header
    echo "INSERT INTO ${table_name} (id, data_field, number_field, created_at) VALUES"
    
    # Generate values
    for ((i=1; i<=$num_records; i++)); do
        local random_string=$(generate_random_string 10)
        local random_number=$((RANDOM % 1000))
        
        if [ $i -eq $num_records ]; then
            echo "($i,'data_$random_string',$random_number,NOW());"
        else
            echo "($i,'data_$random_string',$random_number,NOW()),"
        fi
    done
}


# Function to draw fixed progress displays
draw_progress() {
    local instance_num=$1
    local percent=$2
    local speed=$3
    local items=$4
    local total=$5

    # Ensure synchronized access to the terminal output
    exec 9>/tmp/progress.lock
    flock 9
    
    # Move to correct line and clear it
    tput cup $((instance_num + 5)) 0
    tput el
    
    # Draw progress bar and stats
    local bar=$(create_progress_bar $percent)
    printf "Instance %d: %s %3d%% | %6.1f tables/sec | %4d/%-4d tables\n" \
           $instance_num "$bar" $percent $speed $items $total
    
    # Release the lock
    flock -u 9
    exec 9>&-
}

# Function to create tables for one instance
create_instance_tables() {
    local instance_num=$1
    local start_table=$((instance_num * TABLES_PER_INSTANCE + 1))
    local end_table=$((start_table + TABLES_PER_INSTANCE - 1))
    local instance_start_time=$(get_time_ms)
    local temp_file=$(mktemp)
    local tables_created=0
    
    # Initial display
    draw_progress $instance_num 0 0.0 0 $TABLES_PER_INSTANCE
    
    for ((table_num=start_table; table_num<=end_table; table_num++)); do
        local table_name="perf_test_${table_num}"

        if [ "$DROP_TABLE_IF_EXISTS" = true ]; then
        echo "DROP TABLE IF EXISTS ${table_name};" >> "$temp_file"
        fi

        # Create table
        echo "CREATE TABLE IF NOT EXISTS ${table_name} (
            id INT NOT NULL,
            data_field VARCHAR(255),
            number_field INT,
            created_at TIMESTAMP,
            PRIMARY KEY (id)
        );" >> "$temp_file"
        
        # Insert records if RECORDS_PER_TABLE > 0
        if [ "${RECORDS_PER_TABLE:-0}" -gt 0 ]; then
            generate_insert_data $table_name $RECORDS_PER_TABLE >> "$temp_file"
        fi
        
        ((tables_created++))
        
        if ((tables_created % BATCH_SIZE == 0)) || ((table_num == end_table)); then
            mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$temp_file" 2>/dev/null
            
            local current_time=$(get_time_ms)
            local elapsed=$((current_time - instance_start_time))
            if [ $elapsed -le 0 ]; then
                elapsed=1
            fi
            
            local percent=$((tables_created * 100 / TABLES_PER_INSTANCE))
            local speed=$(echo "scale=1; $tables_created * 1000 / $elapsed" | bc)
            
            draw_progress $instance_num $percent $speed $tables_created $TABLES_PER_INSTANCE
            > "$temp_file"
        fi
    done
    
    rm "$temp_file"
    
    local instance_end_time=$(get_time_ms)
    local instance_duration=$((instance_end_time - instance_start_time))
    
    # Final status display
    exec 9>/tmp/progress.lock
    flock 9
    tput cup $((instance_num + 5)) 0
    tput el
    printf "Instance %d: Completed in %d.%03d seconds\n" \
           $instance_num $((instance_duration/1000)) $((instance_duration%1000))
    flock -u 9
    exec 9>&-
}

# Function to test database connection and handle database creation
test_connection() {
    local start_time=$(get_time_ms)
    local mysql_cmd="mysql -h$DB_HOST -P$DB_PORT -u$DB_USER -p$DB_PASS --connect-timeout=10"
    
    # First test basic connection without database
    if ! $mysql_cmd -e "SELECT 1" 2>/dev/null; then
        echo "Error: Could not connect to MySQL server! Please verify:"
        echo "1. MySQL Docker container is running"
        echo "2. Credentials are correct"
        echo "3. Host '$DB_HOST' is accessible on port $DB_PORT"
        return 1
    fi
    
    # Check if database exists
    if $mysql_cmd -e "USE $DB_NAME" 2>/dev/null; then
        echo "Database '$DB_NAME' exists."
        if [ "$RECREATE_DATABASE" = true ]; then
            echo "Dropping existing database '$DB_NAME'..."
            if $mysql_cmd -e "DROP DATABASE $DB_NAME;" 2>/dev/null; then
                echo "Successfully dropped database '$DB_NAME'"
            else
                echo "Error: Failed to drop database '$DB_NAME'"
                return 1
            fi
        else
            echo "Using existing database '$DB_NAME'"
            return 0
        fi
    fi
    
    # Create database if it doesn't exist or was dropped
    echo "Creating database '$DB_NAME'..."
    if $mysql_cmd -e "CREATE DATABASE $DB_NAME;" 2>/dev/null; then
        echo "Successfully created database '$DB_NAME'"
    else
        echo "Error: Failed to create database '$DB_NAME'"
        echo "Please check if user '$DB_USER' has CREATE DATABASE privileges"
        return 1
    fi
    
    local end_time=$(get_time_ms)
    echo "Database connection and setup completed! Time taken: $((end_time-start_time)) ms"
    return 0
}

# Function to count tables
count_tables() {
    local mysql_cmd="mysql -h$DB_HOST -P$DB_PORT -u$DB_USER -p$DB_PASS $DB_NAME -N -B"
    local query="SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB_NAME' AND table_name LIKE 'perf_test_%'"
    local count=$($mysql_cmd -e "$query" 2>/dev/null)
    
    if [[ $count =~ ^[0-9]+$ ]]; then
        echo "$count"
    else
        echo "0"
    fi
}

# Function to read configuration
read_config() {
    local start_time=$(get_time_ms)
    
    if [ -f "$CONFIG_FILE" ] && [ -z "$DB_PASS" ]; then
        source "$CONFIG_FILE"
        echo "Using configuration from $CONFIG_FILE"
    fi
    
    # Set default values if not specified
    RECREATE_DATABASE=${RECREATE_DATABASE:-false}
    RECREATE_TABLES=${RECREATE_TABLES:-false}
    
    if [ -z "$DB_USER" ] || [ -z "$DB_PASS" ] || [ -z "$DB_NAME" ] || 
       [ -z "$NUM_INSTANCES" ] || [ -z "$TABLES_PER_INSTANCE" ]; then
        echo "Error: Missing required parameters!"
        usage
        exit 1
    fi
    
    local end_time=$(get_time_ms)
    echo "Config loading time: $((end_time-start_time)) ms"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--user)
            DB_USER="$2"
            shift 2
            ;;
        -p|--password)
            DB_PASS="$2"
            shift 2
            ;;
        -h|--host)
            DB_HOST="$2"
            shift 2
            ;;
        -P|--port)
            DB_PORT="$2"
            shift 2
            ;;
        -d|--database)
            DB_NAME="$2"
            shift 2
            ;;
        -i|--instances)
            NUM_INSTANCES="$2"
            shift 2
            ;;
        -t|--tables)
            TABLES_PER_INSTANCE="$2"
            shift 2
            ;;
        -r|--records)
            RECORDS_PER_TABLE="$2"
            shift 2
            ;;
        -b|--batch)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --recreate-db)
            RECREATE_DATABASE=true
            shift
            ;;
        --recreate-tables)
            RECREATE_TABLES=true
            shift
            ;;
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

calculate_tables_per_second() {
    local tables=$1
    local duration=$2
    echo "scale=2; $tables * 1000 / $duration" | bc
}

# Main execution
main() {
    check_environment

    tput clear
    echo "=== Table Creation Started at $(date '+%Y-%m-%d %H:%M:%S') ==="
    
    echo "Reading configuration..."
    read_config
    
    echo "Configuration Summary:"
    echo "- Database: $DB_NAME on $DB_HOST:$DB_PORT"
    echo "- Instances: $NUM_INSTANCES"
    echo "- Tables per instance: $TABLES_PER_INSTANCE"
    echo "- Records per table: ${RECORDS_PER_TABLE:-0}"
    echo "- Batch size: $BATCH_SIZE"
    echo
    
    echo "Testing database connection..."
    if ! test_connection; then
        exit 1
    fi

    echo
    local total_start_time=$(get_time_ms)

    # Prepare lines for progress bars
    for ((i=0; i<NUM_INSTANCES; i++)); do
        echo
    done

    # Start instances
    for ((i=0; i<NUM_INSTANCES; i++)); do
        create_instance_tables $i &
    done

    # Wait for all processes to complete
    wait

    # Move cursor to end of progress area
    tput cup $((NUM_INSTANCES + 6)) 0
    
    local total_end_time=$(get_time_ms)
    local total_duration=$((total_end_time - total_start_time))
    
    echo "=== Table Creation Completed at $(date '+%Y-%m-%d %H:%M:%S') ==="
    echo "Final Summary:"
    echo "- Total execution time: $((total_duration/1000)).$((total_duration%1000)) seconds"
    
    echo "Verifying actual table count..."
    local actual_tables=$(count_tables)
    if [[ $actual_tables =~ ^[0-9]+$ ]]; then
        echo "- Total tables created: $actual_tables"
        if [ "$actual_tables" -eq "$((NUM_INSTANCES * TABLES_PER_INSTANCE))" ]; then
            echo "✓ Table count matches expected number"
        else
            echo "! Warning: Table count mismatch"
            echo "  Expected: $((NUM_INSTANCES * TABLES_PER_INSTANCE))"
            echo "  Created: $actual_tables"
        fi
        
        if [ "$actual_tables" -gt 0 ]; then
            # echo "- Tables created per second: $(awk "BEGIN {printf \"%.2f\", $actual_tables * 1000 / $total_duration}")"
            local tables_per_second=$(calculate_tables_per_second $actual_tables $total_duration)
            echo "- Tables created per second: ${tables_per_second}"
        fi
    else
        echo "Error: Could not verify table count"
    fi
}

# Execute main function
main