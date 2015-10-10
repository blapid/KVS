# CONFIGURATIONS

PROJ_NAME=kvs
DEMO_NAME=demo

SRC_DIR=src
INC_DIR=inc
OBJ_DIR=obj
EXE_DIR=exe

CC=gcc
AR=ar
SO=$(CC)
LD=$(CC)

CC_FLAGS=-I$(INC_DIR) -fPIC -Wall -Werror -Os -g3
AR_FLAGS=rcs
SO_FLAGS=-shared
LD_FLAGS=

# RULES

all: static shared demo

static: dirs $(EXE_DIR)/lib$(PROJ_NAME).a

shared: dirs $(EXE_DIR)/lib$(PROJ_NAME).so

demo: dirs $(EXE_DIR)/$(DEMO_NAME)

dirs:
	mkdir -p $(OBJ_DIR) $(EXE_DIR)

$(EXE_DIR)/lib$(PROJ_NAME).a: $(OBJ_DIR)/$(PROJ_NAME).o
	$(AR) $(AR_FLAGS) $@ $<

$(EXE_DIR)/lib$(PROJ_NAME).so: $(OBJ_DIR)/$(PROJ_NAME).o
	$(SO) $(SO_FLAGS) -o $@ $<

$(EXE_DIR)/$(DEMO_NAME): $(OBJ_DIR)/$(PROJ_NAME).o $(OBJ_DIR)/$(DEMO_NAME).o
	$(LD) $(LD_FLAGS) -o $@ $^

$(OBJ_DIR)/$(PROJ_NAME).o: $(SRC_DIR)/$(PROJ_NAME).c $(INC_DIR)/$(PROJ_NAME).h
	$(CC) -c $(CC_FLAGS) -o $@ $<

$(OBJ_DIR)/$(DEMO_NAME).o: $(SRC_DIR)/$(DEMO_NAME).c $(INC_DIR)/$(PROJ_NAME).h
	$(CC) -c $(CC_FLAGS) -o $@ $<

clean:
	rm -r $(OBJ_DIR) $(EXE_DIR) > /dev/null 2>&1
