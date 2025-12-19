"""
MeshChain ESP32 Storage Module (Full Node)

This module provides a simple, file-based storage system for a MeshChain full node
running on an ESP32. It is designed to store the entire blockchain on a microSD card.
"""

import os
import json
from pathlib import Path
from typing import Optional

from meshchain.core.block import Block

class FullNodeStorage:
    """
    Manages the storage of the full blockchain on a microSD card.

    The directory structure is simple:
    - <db_path>/blocks/ - Contains individual block files, named by height.
    - <db_path>/state.json - Contains metadata like the current chain height.
    """

    def __init__(self, db_path: str = "/mnt/microsd/blockchain"):
        """Initializes the storage system."""
        self.db_path = Path(db_path)
        self.blocks_path = self.db_path / "blocks"
        self.state_file = self.db_path / "state.json"

        self.db_path.mkdir(parents=True, exist_ok=True)
        self.blocks_path.mkdir(exist_ok=True)

    def save_block(self, block: Block) -> bool:
        """Saves a block to the microSD card."""
        block_file = self.blocks_path / f"{block.height}.json"
        try:
            with open(block_file, 'w') as f:
                # Note: A binary format would be more efficient than JSON.
                json.dump(block.to_dict(), f)  # Assumes Block has a to_dict() method
            self._update_state(block.height)
            return True
        except IOError as e:
            print(f"Error saving block {block.height}: {e}")
            return False

    def get_block(self, height: int) -> Optional[Block]:
        """Retrieves a block from the microSD card by its height."""
        block_file = self.blocks_path / f"{height}.json"
        if not block_file.exists():
            return None
        try:
            with open(block_file, 'r') as f:
                block_data = json.load(f)
                # Assumes Block can be instantiated from a dict
                return Block.from_dict(block_data)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading block {height}: {e}")
            return None

    def get_latest_block(self) -> Optional[Block]:
        """Retrieves the latest block from the blockchain."""
        latest_height = self._get_latest_height()
        if latest_height == -1:
            return None
        return self.get_block(latest_height)

    def _get_latest_height(self) -> int:
        """Reads the latest block height from the state file."""
        if not self.state_file.exists():
            return -1
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                return state.get('latest_block_height', -1)
        except (IOError, json.JSONDecodeError):
            return -1

    def _update_state(self, new_height: int):
        """Updates the state file with the new latest block height."""
        current_height = self._get_latest_height()
        if new_height > current_height:
            state = {'latest_block_height': new_height}
            try:
                with open(self.state_file, 'w') as f:
                    json.dump(state, f)
            except IOError as e:
                print(f"Error updating state file: {e}")

# Note: The Block class will need to be updated to include to_dict() and from_dict() methods
# for this storage mechanism to work.
