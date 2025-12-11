import os
import subprocess
import tempfile
from pathlib import Path
from typing import Iterator
from cog import BasePredictor, Input, Path as CogPath

class Predictor(BasePredictor):
    def setup(self):
        """Setup runs once when the container starts"""
        # Verify p7zip is installed
        try:
            subprocess.run(["7z"], capture_output=True)
            print("✓ p7zip (7z) is available")
        except FileNotFoundError:
            raise RuntimeError("7z command not found. Ensure p7zip-full is installed.")

    def predict(
        self,
        input_files: list[CogPath] = Input(
            description="Files to compress (supports multiple files)",
        ),
        compression_level: int = Input(
            description="Compression level (0=store, 1=fastest, 3=fast, 5=normal, 7=maximum, 9=ultra)",
            default=5,
            ge=0,
            le=9,
        ),
        compression_method: str = Input(
            description="Compression method (LZMA2 for 7z, Deflate for zip recommended)",
            default="LZMA2",
            choices=["LZMA", "LZMA2", "PPMd", "BZip2", "Deflate", "Copy"],
        ),
        archive_format: str = Input(
            description="Archive format (7z recommended for best compression)",
            default="7z",
            choices=["7z", "zip", "tar"],
        ),
        password: str = Input(
            description="Optional password for encryption (leave empty for no encryption)",
            default="",
        ),
        solid_archive: bool = Input(
            description="Create solid archive (better compression, slower)",
            default=True,
        ),
        volume_size: str = Input(
            description="Split archive into volumes (e.g., '100m', '1g', leave empty for single file)",
            default="",
        ),
    ) -> CogPath:
        """Compress files using p7zip with customizable options"""
        
        print("\n" + "="*60)
        print("🗜️  P7ZIP FILE COMPRESSION")
        print("="*60)
        
        # Create output directory in /tmp (not auto-cleaned)
        import uuid
        output_dir = Path(f"/tmp/p7zip_output_{uuid.uuid4().hex[:8]}")
        output_dir.mkdir(exist_ok=True)
        
        # Copy input files to temp directory
        print(f"\n📥 Processing {len(input_files)} input file(s)...")
        file_list = []
        total_size = 0
        
        # Validate format and method compatibility
        if archive_format == "zip" and compression_method == "LZMA2":
            print("⚠️  Warning: ZIP doesn't support LZMA2, using LZMA instead")
            compression_method = "LZMA"
        
        for idx, input_file in enumerate(input_files, 1):
            src_path = Path(str(input_file))
            
            # Verify file exists
            if not src_path.exists():
                raise RuntimeError(f"Input file does not exist: {src_path}")
            
            file_size = src_path.stat().st_size
            total_size += file_size
            
            print(f"  [{idx}] {src_path.name} ({self._format_size(file_size)})")
            file_list.append(str(src_path.absolute()))
        
        print(f"\n📊 Total input size: {self._format_size(total_size)}")
        
        # Determine output filename
        output_name = f"compressed.{archive_format}"
        if volume_size:
            output_name = f"compressed.{archive_format}.001"
        
        output_path = output_dir / output_name
        
        # Build 7z command
        cmd = ["7z", "a"]  # 'a' = add to archive
        
        # Archive format
        cmd.extend([f"-t{archive_format}"])
        
        # Compression level
        cmd.extend([f"-mx={compression_level}"])
        print(f"\n⚙️  Compression level: {compression_level} ({self._get_level_name(compression_level)})")
        
        # Compression method (only for 7z format)
        if archive_format == "7z":
            cmd.extend([f"-m0={compression_method}"])
            print(f"⚙️  Compression method: {compression_method}")
            
            # Solid archive (only for 7z)
            if solid_archive:
                cmd.extend(["-ms=on"])
                print(f"⚙️  Solid archive: enabled")
            else:
                cmd.extend(["-ms=off"])
        elif archive_format == "zip":
            # ZIP uses different method syntax
            zip_methods = {
                "LZMA": "-mm=LZMA",
                "LZMA2": "-mm=LZMA",  # ZIP doesn't support LZMA2, use LZMA
                "PPMd": "-mm=PPMd",
                "BZip2": "-mm=BZip2",
                "Deflate": "-mm=Deflate",
                "Copy": "-mm=Copy"
            }
            cmd.extend([zip_methods.get(compression_method, "-mm=Deflate")])
            print(f"⚙️  Compression method: {compression_method}")
        else:
            # TAR doesn't have compression methods in 7z
            print(f"⚙️  Archive format: tar (compression via level only)")
        
        # Password encryption
        if password:
            cmd.extend([f"-p{password}"])
            if archive_format == "7z":
                cmd.extend(["-mhe=on"])  # Encrypt headers
            print(f"🔒 Password protection: enabled")
        
        # Volume splitting
        if volume_size:
            cmd.extend([f"-v{volume_size}"])
            print(f"📦 Volume size: {volume_size}")
        
        # Progress and other options
        cmd.extend([
            "-bsp1",  # Show progress
            "-bt",    # Show execution time
        ])
        
        # Output file
        cmd.append(str(output_path))
        
        # Input files
        cmd.extend(file_list)
        
        print(f"\n🚀 Starting compression...")
        print(f"📦 Output format: {archive_format.upper()}")
        if len(cmd) < 20:  # Only show full command if reasonable length
            print(f"Command: {' '.join(cmd)}")
        else:
            print(f"Command: {' '.join(cmd[:10])}... [+{len(file_list)} files]")
        print("-" * 60)
        
        # Run compression with real-time output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        
        # Stream output
        stdout_lines = []
        stderr_lines = []
        
        # Read stdout
        if process.stdout:
            for line in process.stdout:
                line = line.rstrip()
                stdout_lines.append(line)
                if line:
                    # Parse progress if available
                    if "%" in line:
                        print(f"⏳ {line}")
                    elif any(x in line for x in ["Everything", "OK", "Compressing", "Adding", "files,"]):
                        print(f"✓ {line}")
                    elif "ERROR" in line or "Error" in line:
                        print(f"❌ {line}")
                    else:
                        print(f"  {line}")
        
        # Wait for process to complete
        stderr_output = process.stderr.read() if process.stderr else ""
        process.wait()
        
        # Check for errors
        if process.returncode != 0:
            print("\n" + "="*60)
            print("❌ COMPRESSION FAILED")
            print("="*60)
            print(f"\nExit code: {process.returncode}")
            
            if stderr_output:
                print(f"\nError output:")
                print(stderr_output)
            
            print("\n7z command that was attempted:")
            print(" ".join(cmd))
            
            print("\nFile paths being compressed:")
            for f in file_list[:5]:  # Show first 5
                print(f"  - {f}")
            if len(file_list) > 5:
                print(f"  ... and {len(file_list) - 5} more")
            
            raise RuntimeError(
                f"Compression failed with exit code {process.returncode}. "
                f"Check the output above for details."
            )
        
        print("-" * 60)
        print("✅ Compression completed successfully!")
        
        # Check output file(s)
        if volume_size:
            # Find all volume files
            volume_files = sorted(output_dir.glob(f"compressed.{archive_format}.*"))
            if not volume_files:
                raise RuntimeError(
                    f"No output volumes found in {output_dir}\n"
                    f"Expected pattern: compressed.{archive_format}.*"
                )
            
            total_compressed = sum(f.stat().st_size for f in volume_files)
            print(f"\n📦 Created {len(volume_files)} volume(s)")
            for vol in volume_files:
                print(f"  • {vol.name} ({self._format_size(vol.stat().st_size)})")
            
            # For volumes, return the first one (Cog limitation - single file output)
            final_output = volume_files[0]
            print(f"\n⚠️  Returning first volume: {final_output.name}")
            print(f"   Note: Download all volumes manually to extract")
        else:
            if not output_path.exists():
                raise RuntimeError(
                    f"Output file was not created: {output_path}\n"
                    f"Compression may have failed silently."
                )
            final_output = output_path
            compressed_size = final_output.stat().st_size
            total_compressed = compressed_size
        
        # Calculate compression ratio
        ratio = (1 - total_compressed / total_size) * 100 if total_size > 0 else 0
        
        print(f"\n📊 COMPRESSION STATISTICS")
        print(f"  Original size:   {self._format_size(total_size)}")
        print(f"  Compressed size: {self._format_size(total_compressed)}")
        print(f"  Compression ratio: {ratio:.1f}% reduction")
        print(f"  Space saved: {self._format_size(total_size - total_compressed)}")
        
        print("\n" + "="*60)
        print("✨ Process complete!")
        print("="*60 + "\n")
        
        return CogPath(final_output)
    
    def _format_size(self, size_bytes: int) -> str:
        """Format bytes into human-readable size"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def _get_level_name(self, level: int) -> str:
        """Get compression level name"""
        names = {
            0: "Store (no compression)",
            1: "Fastest",
            3: "Fast",
            5: "Normal",
            7: "Maximum",
            9: "Ultra"
        }
        return names.get(level, f"Level {level}")