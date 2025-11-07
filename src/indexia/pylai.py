"""Manages indexia data entry."""

from PIL import Image
import pytesseract

class Grammascope:
    """Process images to extract text data.
    
    :param model_dir: Directory containing the OCR model files
    :type model_dir: str
    :param model_name: Name of the OCR model to use
    :type model_name: str
    """

    def __init__(
        self,
        model_dir: str,
        model_name: str
    ):
        self.model_dir = model_dir
        self.model_name = model_name

    def extract_text(
        self,
        image_path: str,
        timeout: int = 30
    ) -> str:
        """Extract text from the image using OCR.
        
        Uses the Tesseract OCR engine with a custom language model to extract
        text from the provided image.
        
        :param image_path: Path to the image file to process
        :type image_path: str
        :param timeout: Number of seconds to wait for OCR processing before timing out (0 for no timeout)
        :type timeout: int
        :return: The extracted text from the image
        :rtype: str
        :raises FileNotFoundError: If the image file does not exist
        :raises pytesseract.TesseractError: If OCR processing fails
        :raises RuntimeError: If OCR processing exceeds the timeout duration
        """
        image = Image.open(image_path)

        config = f'--tessdata-dir "{self.model_dir}"'
        text = pytesseract.image_to_string(
            image,
            lang=self.model_name,
            config=config,
            timeout=timeout
        )

        return text